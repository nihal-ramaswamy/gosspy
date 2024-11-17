package org.gosspy.gossiper;

import com.google.protobuf.Any;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.AllArgsConstructor;
import org.gosspy.config.GosspyConfig;
import org.gosspy.dto.Data;
import org.gosspy.gen.protobuf.GossipRequest;
import org.gosspy.gen.protobuf.GossipResponse;
import org.gosspy.gen.protobuf.ResponseStatus;
import org.gosspy.gen.protobuf.RpcGossipHandlerGrpc;
import org.gosspy.utils.Miscellaneous;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@AllArgsConstructor
public class RpcNetworkHandler<K, T> extends RpcGossipHandlerGrpc.RpcGossipHandlerImplBase {

    private final Data<K, T> dataHandler;
    private final RpcDataHandler<K, T> rpcDataHandler;
    private final GosspyConfig gosspyConfig;

    private GossipResponse sendGrpcRequest(GossipRequest request, URI uri) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(uri.getHost(), uri.getPort()).usePlaintext().build();
        RpcGossipHandlerGrpc.RpcGossipHandlerBlockingStub stub = RpcGossipHandlerGrpc.newBlockingStub(channel);
        GossipResponse response = stub.getData(request);
        channel.shutdown();
        return response;
    }


    @Override
    public void getData(GossipRequest request, StreamObserver<GossipResponse> responseObserver) {
        Long id = request.getId();
        K key = (K) request.getKey();

        int count = request.getCount();
        Optional<T> data = this.rpcDataHandler.getData(key, id);

        if (data.isPresent()) {
            count++;
        }

        for (var server : Miscellaneous.copyShuffle(gosspyConfig.nodes().servers())) {
            if (count >= gosspyConfig.nodes().reads()) {
                break;
            }

            GossipResponse response = sendGrpcRequest(request, server);
            if (response.getStatus() != ResponseStatus.ACCEPTED) {
                continue;
            }

            if (data.isPresent() && response.hasData()) {
                data = Optional.of(this.dataHandler.getLatestVersion(data.get(), (T) response.getData()));
            }

            if (data.isEmpty()) {
                data = Optional.of((T) response.getData());
            }

            count++;
        }

        if (data.isEmpty() || count < gosspyConfig.nodes().reads()) {
            responseObserver.onNext(GossipResponse.newBuilder()
                    .setId(request.getId())
                    .setStatus(ResponseStatus.REJECTED)
                    .build());
            responseObserver.onCompleted();
            return;
        }

        responseObserver.onNext(GossipResponse.newBuilder()
                .setId(request.getId())
                .setData((Any) data.get())
                .setStatus(ResponseStatus.ACCEPTED)
                .build());
        responseObserver.onCompleted();
        return;
    }

    @Override
    public void setData(GossipRequest request, StreamObserver<GossipResponse> responseObserver) {
        Long id = request.getId();
        K key = (K) request.getKey();
        T data = (T) request.getData();
        int count = 0;

        if (this.rpcDataHandler.setData(key, data, id)) {
            count++;
        }

        for (var server : Miscellaneous.copyShuffle(gosspyConfig.nodes().servers())) {
            if (count >= gosspyConfig.nodes().writes()) {
                break;
            }
            GossipResponse response = sendGrpcRequest(request, server);
            if (response.getStatus() != ResponseStatus.ACCEPTED) {
                continue;
            }
            count++;
        }

        ResponseStatus status = count >= gosspyConfig.nodes().writes() ? ResponseStatus.ACCEPTED : ResponseStatus.REJECTED;
        responseObserver.onNext(GossipResponse.newBuilder()
                .setId(request.getId())
                .setStatus(status)
                .build());
        responseObserver.onCompleted();
    }
}