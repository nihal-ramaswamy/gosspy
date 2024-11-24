package org.gosspy.gossiper;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gosspy.config.GosspyConfig;
import org.gosspy.db.RequestsHistoryDb;
import org.gosspy.dto.Data;
import org.gosspy.gen.protobuf.GossipRequest;
import org.gosspy.gen.protobuf.GossipResponse;
import org.gosspy.gen.protobuf.ResponseStatus;
import org.gosspy.gen.protobuf.RpcGossipHandlerGrpc;
import org.gosspy.utils.Miscellaneous;

import java.net.URI;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@AllArgsConstructor
@Slf4j
public class RpcNetworkHandler extends RpcGossipHandlerGrpc.RpcGossipHandlerImplBase {

    private final Data dataHandler;
    private final RpcDataHandler rpcDataHandler;
    private final GosspyConfig gosspyConfig;

    private GossipResponse sendGrpcRequestGetData(GossipRequest request, URI uri) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(uri.getHost(), uri.getPort()).usePlaintext().build();
        RpcGossipHandlerGrpc.RpcGossipHandlerBlockingStub stub = RpcGossipHandlerGrpc.newBlockingStub(channel);
        GossipResponse response = stub.getData(request);
        channel.shutdown();
        return response;
    }

    private GossipResponse sendGrpcRequestSetData(GossipRequest request, URI uri) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(uri.getHost(), uri.getPort()).usePlaintext().build();
        RpcGossipHandlerGrpc.RpcGossipHandlerBlockingStub stub = RpcGossipHandlerGrpc.newBlockingStub(channel);
        GossipResponse response = stub.getData(request);
        channel.shutdown();
        return response;
    }

    private static boolean checkIfResponseAccepted(Long messageId, Integer currentNodeId, StreamObserver<GossipResponse> responseObserver) {
        RequestsHistoryDb requestsHistoryDb = RequestsHistoryDb.getInstance();

        try {
            if (requestsHistoryDb.getRequestStatus(messageId, currentNodeId) == ResponseStatus.ACCEPTED) {
                log.atDebug().addKeyValue("id", messageId).addKeyValue("nodeId", currentNodeId).log("Request id already processed.");
                responseObserver.onNext(GossipResponse.newBuilder()
                        .setId(messageId)
                        .setStatus(ResponseStatus.ACCEPTED)
                        .build());
                responseObserver.onCompleted();
                return true;
            }
        } catch (SQLException e) {
            log.atError().addKeyValue("message", e.getMessage()).log("Error getting data.");
        }

        return false;
    }

    private static boolean checkIfResponseProcessing(Long messageId, Integer currentNodeId, StreamObserver<GossipResponse> responseObserver) {
        RequestsHistoryDb requestsHistoryDb = RequestsHistoryDb.getInstance();

        // If request sent again before node finishes processing, reject it.
        try {
            if (requestsHistoryDb.getRequestStatus(messageId, currentNodeId) == ResponseStatus.PROCESSING) {
                log.atDebug().addKeyValue("id", messageId).addKeyValue("nodeId", currentNodeId).log("Got duplicate request while still processing.");
                responseObserver.onNext(GossipResponse.newBuilder()
                        .setId(messageId)
                        .setStatus(ResponseStatus.PROCESSING)
                        .build());
                responseObserver.onCompleted();
                return true;
            }
        } catch (SQLException e) {
            log.atError().addKeyValue("message", e.getMessage()).log("Error getting data");
        }

        return false;
    }

    private static boolean updateStatusToProcessingIfChecksPassed(Long messageId, Integer currentNodeId, StreamObserver<GossipResponse> responseObserver) {
        RequestsHistoryDb requestsHistoryDb = RequestsHistoryDb.getInstance();

        if (RpcNetworkHandler.checkIfResponseProcessing(messageId, currentNodeId, responseObserver) ||
            RpcNetworkHandler.checkIfResponseAccepted(messageId, currentNodeId, responseObserver)) {
            return true;
        }

        // Update db to status processing
        try {
            requestsHistoryDb.upsertIntoRequests(messageId, currentNodeId, ResponseStatus.PROCESSING);
        } catch (SQLException e) {
            log.atError().addKeyValue("message", e.getMessage()).log("Error updating status to processing");
        }

        return false;
    }



    @Override
    public void getData(GossipRequest request, StreamObserver<GossipResponse> responseObserver) {
        RequestsHistoryDb requestsHistoryDb = RequestsHistoryDb.getInstance();
        Integer currentNode = gosspyConfig.nodes().current().id();

        if (RpcNetworkHandler.updateStatusToProcessingIfChecksPassed(request.getId(), currentNode, responseObserver)) {
            return;
        }

        Long id = request.getId();
        String key = request.getKey();


        AtomicInteger count = new AtomicInteger(request.getCount());
        AtomicReference<Optional<String>> data = new AtomicReference<>(this.rpcDataHandler.getData(key, id));

        if (data.get().isPresent()) {
            count.incrementAndGet();
        }

        // Query other nodes in random order.
        Miscellaneous.copyShuffle(gosspyConfig.nodes().servers()).parallelStream().forEach((server) -> {
            if (count.get() >= gosspyConfig.nodes().reads()) {
                return;
            }

            if (server.equals(gosspyConfig.nodes().current().address())) {
                return;
            }

            log.atInfo().addKeyValue("host", server).addKeyValue("key", request.getKey()).log("Sending getData request from getData");
            GossipResponse response = sendGrpcRequestGetData(request, server);

            if (response.getStatus() == ResponseStatus.ACCEPTED) {
                if (data.get().isPresent() && response.hasData()) {
                    data.set(Optional.of(this.dataHandler.getLatestVersion(data.get().get(), response.getData())));
                }

                if (data.get().isEmpty()) {
                    data.set(Optional.of(response.getData()));
                }
                count.incrementAndGet();
            }
        });

        // If data is empty or not enough nodes read, return reject
        if (data.get().isEmpty() || count.get() < gosspyConfig.nodes().reads()) {
            responseObserver.onNext(GossipResponse.newBuilder()
                    .setId(request.getId())
                    .setStatus(ResponseStatus.REJECTED)
                    .build());
            responseObserver.onCompleted();

            try {
                requestsHistoryDb.upsertIntoRequests(request.getId(), currentNode, ResponseStatus.REJECTED);
            } catch (SQLException e) {
                log.atError().addKeyValue("message", e.getMessage()).log("Error updating status to rejected");
            }

            return;
        }

        // Send an accepted status and update db
        responseObserver.onNext(GossipResponse.newBuilder()
                .setId(request.getId())
                .setData(data.get().get())
                .setStatus(ResponseStatus.ACCEPTED)
                .build());
        responseObserver.onCompleted();

        try {
            requestsHistoryDb.upsertIntoRequests(request.getId(), currentNode, ResponseStatus.ACCEPTED);
        } catch (SQLException e) {
            log.atError().addKeyValue("message", e.getMessage()).log("Error updating status to accepted");
        }

    }

    @Override
    public void setData(GossipRequest request, StreamObserver<GossipResponse> responseObserver) {
        long id = request.getId();
        String key = request.getKey();
        String data = request.getData();
        AtomicInteger count = new AtomicInteger(0);
        Integer currentNode = gosspyConfig.nodes().current().id();
        RequestsHistoryDb requestsHistoryDb = RequestsHistoryDb.getInstance();

        if (RpcNetworkHandler.updateStatusToProcessingIfChecksPassed(request.getId(), currentNode, responseObserver)) {
            return;
        }

        // Set data for itself
        if (this.rpcDataHandler.setData(key, data, id)) {
            count.incrementAndGet();
        }

        // Send data to other nodes until minimum count has reached.
        Miscellaneous.copyShuffle(gosspyConfig.nodes().servers()).parallelStream().forEach((server) -> {
            if (count.get() >= gosspyConfig.nodes().writes()) {
                return;
            }

            if (server.equals(gosspyConfig.nodes().current().address())) {
                return;
            }

            log.atInfo().addKeyValue("host", server).addKeyValue("key", request.getKey()).log("Sending getData request from setData");

            GossipResponse response = sendGrpcRequestSetData(request, server);
            if (response.getStatus() == ResponseStatus.ACCEPTED) {
                count.incrementAndGet();
            }
        });


        ResponseStatus status = count.get() >= gosspyConfig.nodes().writes() ? ResponseStatus.ACCEPTED : ResponseStatus.REJECTED;
        responseObserver.onNext(GossipResponse.newBuilder()
                .setId(request.getId())
                .setStatus(status)
                .build());
        responseObserver.onCompleted();

        try {
            requestsHistoryDb.upsertIntoRequests(request.getId(), currentNode, status);
        } catch (SQLException e) {
            log.atError().addKeyValue("message", e.getMessage()).addKeyValue("status", status).log("Error updating status");
        }
    }
}