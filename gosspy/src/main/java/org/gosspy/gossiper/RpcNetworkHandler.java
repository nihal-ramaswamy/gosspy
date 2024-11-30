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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

@AllArgsConstructor
@Slf4j
public class RpcNetworkHandler extends RpcGossipHandlerGrpc.RpcGossipHandlerImplBase {

    private final Data dataHandler;
    private final RpcDataHandler rpcDataHandler;
    private final GosspyConfig gosspyConfig;

    /**
     * Send getData RPC request
     *
     * @param request {@link GossipRequest} Request object
     * @param uri {@link URI} The uri to send the request to
     * @return {@link GossipResponse} The response after sending the request
     */
    private GossipResponse sendGrpcRequestGetData(GossipRequest request, URI uri) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(uri.getHost(), uri.getPort()).usePlaintext().build();
        RpcGossipHandlerGrpc.RpcGossipHandlerBlockingStub stub = RpcGossipHandlerGrpc.newBlockingStub(channel);
        GossipResponse response = stub.getData(request);
        channel.shutdown();
        return response;
    }

    /**
     * Send setData RPC request
     *
     * @param request {@link GossipRequest} Request object
     * @param uri {@link URI} The uri to send the request to
     * @return {@link GossipResponse} The response after sending the request
     */
    private GossipResponse sendGrpcRequestSetData(GossipRequest request, URI uri) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(uri.getHost(), uri.getPort()).usePlaintext().build();
        RpcGossipHandlerGrpc.RpcGossipHandlerBlockingStub stub = RpcGossipHandlerGrpc.newBlockingStub(channel);
        GossipResponse response = stub.setData(request);
        channel.shutdown();
        return response;
    }

    /**
     * Checks if there is an entry for the message id and node with a status of {@code ResponseStatus.ACCEPTED}
     *
     * @param messageId        {@link Long} the message id
     * @param currentNodeId    {@link Integer} the node id
     * @param responseObserver {@link StreamObserver<GossipResponse>} used to handle the grpc request
     * @return {@link Boolean} {@code true} if entry exists, else {@code false}
     */
    private static Boolean checkIfResponseAccepted(Long messageId, Integer currentNodeId, StreamObserver<GossipResponse> responseObserver) {
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

    /**
     * Checks if there is an entry for the message id and node with a status of {@code ResponseStatus.PROCESSING}
     *
     * @param messageId        {@link Long} the message id
     * @param currentNodeId    {@link Integer} the node id
     * @param responseObserver {@link StreamObserver<GossipResponse>} used to handle the grpc request
     * @return {@link Boolean} {@code true} if entry exists, else {@code false}
     */
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

    /**
     * If both {@link RpcNetworkHandler::checkIfResponseProcessing} and {@link RpcNetworkHandler::checkIfResponseAccepted} both return false, it will upsert into REQUESTS_DB.
     *
     * @param messageId        {@link Long} the message id
     * @param currentNodeId    {@link Integer} the node id
     * @param responseObserver {@link StreamObserver<GossipResponse>} used to handle the grpc request
     * @return {@link Boolean} {@code false} if entry was upserted, else {@code true}
     */
    private static Boolean updateStatusToProcessingIfChecksPassed(Long messageId, Integer currentNodeId, StreamObserver<GossipResponse> responseObserver) {
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


    /**
     * Fetches data by querying other nodes along with itself.
     * It queries a minimum of {@code gosspyConfig.nodes().reads()} nodes defined from the application.yml file
     * Returns a {@code ResponseStatus.ACCEPTED} or {@code ResponseStatus.REJECTED} based on the processing.
     *
     * @param request {@link GossipRequest} The request that needs to be processed
     * @param responseObserver {@link StreamObserver<GossipResponse>} grpc response observer
     */
    @Override
    public void getData(GossipRequest request, StreamObserver<GossipResponse> responseObserver) {
        RequestsHistoryDb requestsHistoryDb = RequestsHistoryDb.getInstance();
        Integer currentNode = gosspyConfig.nodes().current().id();
        Long id = request.getId();
        String key = request.getKey();

        if (RpcNetworkHandler.updateStatusToProcessingIfChecksPassed(request.getId(), currentNode, responseObserver)) {
            return;
        }

        AtomicReference<List<String>> serversQueried = new AtomicReference<>(request.getServersList());
        AtomicReference<Optional<String>> data = new AtomicReference<>(this.rpcDataHandler.getData(key, id));

        if (data.get().isPresent()) {
            String currentNodeUriString = gosspyConfig.nodes().current().address().toString();
            if (!Miscellaneous.isElementInAtomicReferenceArray(serversQueried, currentNodeUriString)) {
                serversQueried.get().add(currentNodeUriString);
            }
        }

        // Query other nodes in random order.
        Miscellaneous.copyShuffle(gosspyConfig.nodes().servers()).parallelStream().forEach((server) -> {
            if (serversQueried.get().size() >= gosspyConfig.nodes().reads() ||
                Miscellaneous.isElementInAtomicReferenceArray(serversQueried, server.toString())) {
                return;
            }

            GossipRequest.Builder newGossipRequestBuilder = GossipRequest.newBuilder()
                .setId(request.getId())
            .setKey(request.getKey())
            .setData(request.getData());

            int idx = 0;
            for (String servers: serversQueried.get()) {
                newGossipRequestBuilder.setServers(idx, servers);
                idx++;
            }


            log.atInfo().addKeyValue("host", server).addKeyValue("key", request.getKey()).log("Sending getData request from getData");
            GossipResponse response = sendGrpcRequestGetData(newGossipRequestBuilder.build(), server);

            if (response.getStatus() == ResponseStatus.ACCEPTED) {
                if (data.get().isPresent() && response.hasData()) {
                    data.set(Optional.of(this.dataHandler.getLatestVersion(data.get().get(), response.getData())));
                }

                if (data.get().isEmpty()) {
                    data.set(Optional.of(response.getData()));
                }
                serversQueried.get().add(server.toString());
            }
        });

        // If data is empty or not enough nodes read, return reject
        if (data.get().isEmpty() || serversQueried.get().size() < gosspyConfig.nodes().reads()) {
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
            log.atError().addKeyValue("message", e.getMessage()).addKeyValue("status", ResponseStatus.ACCEPTED).log("Error updating status");
        }
    }

    /**
     * Sends data by querying other nodes along with itself.
     * It queries a minimum of {@code gosspyConfig.nodes().reads()} nodes defined from the application.yml file
     * Returns a {@code ResponseStatus.ACCEPTED} or {@code ResponseStatus.REJECTED} based on the processing.
     *
     * @param request {@link GossipRequest} The request that needs to be processed
     * @param responseObserver {@link StreamObserver<GossipResponse>} grpc response observer
     */
    @Override
    public void setData(GossipRequest request, StreamObserver<GossipResponse> responseObserver) {
        long id = request.getId();
        String key = request.getKey();
        String data = request.getData();
        Integer currentNode = gosspyConfig.nodes().current().id();
        RequestsHistoryDb requestsHistoryDb = RequestsHistoryDb.getInstance();

        if (RpcNetworkHandler.updateStatusToProcessingIfChecksPassed(request.getId(), currentNode, responseObserver)) {
            return;
        }

        AtomicReference<List<String>> serversQueried = new AtomicReference<>(new ArrayList<>(request.getServersList()));


        // Set data for itself
        if (this.rpcDataHandler.setData(key, data, id)) {
            String currentNodeUriString = gosspyConfig.nodes().current().address().toString();
            if (!Miscellaneous.isElementInAtomicReferenceArray(serversQueried, currentNodeUriString)) {
                serversQueried.get().add(currentNodeUriString);
            }
        }

        // Send data to other nodes until minimum count has reached.
        Miscellaneous.copyShuffle(gosspyConfig.nodes().servers()).parallelStream().forEach((server) -> {
            if (serversQueried.get().size() >= gosspyConfig.nodes().writes() ||
                Miscellaneous.isElementInAtomicReferenceArray(serversQueried, server.toString())) {
                return;
            }

            GossipRequest.Builder newGossipRequestBuilder = GossipRequest.newBuilder()
                .setId(request.getId())
            .setKey(request.getKey())
            .setData(request.getData());


            newGossipRequestBuilder.addAllServers(serversQueried.get());

            log.atInfo().addKeyValue("host", server).addKeyValue("key", request.getKey()).log("Sending getData request from setData");

            GossipResponse response = sendGrpcRequestSetData(newGossipRequestBuilder.build(), server);

            if (response.getStatus() == ResponseStatus.ACCEPTED) {
                serversQueried.get().add(server.toString());
            }
        });


        ResponseStatus status = serversQueried.get().size() >= gosspyConfig.nodes().writes() ? ResponseStatus.ACCEPTED : ResponseStatus.REJECTED;
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