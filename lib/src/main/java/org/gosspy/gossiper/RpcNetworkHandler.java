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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

// TODO: Find a better way instead of suppressing warnings.
@AllArgsConstructor
@SuppressWarnings("unchecked")
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


        AtomicInteger count = new AtomicInteger(request.getCount());
        AtomicReference<Optional<T>> data = new AtomicReference<>(this.rpcDataHandler.getData(key, id));

        if (data.get().isPresent()) {
            count.incrementAndGet();
        }

        Miscellaneous.copyShuffle(gosspyConfig.nodes().servers()).parallelStream().forEach((server) -> {
            if (count.get() >= gosspyConfig.nodes().reads()) {
                return;
            }

            GossipResponse response = sendGrpcRequest(request, server);

            if (response.getStatus() == ResponseStatus.ACCEPTED) {
                if (data.get().isPresent() && response.hasData()) {
                    data.set(Optional.of(this.dataHandler.getLatestVersion(data.get().get(), (T) response.getData())));
                }

                if (data.get().isEmpty()) {
                    data.set(Optional.of((T) response.getData()));
                }
                count.incrementAndGet();
            }

        });

        if (data.get().isEmpty() || count.get() < gosspyConfig.nodes().reads()) {
            responseObserver.onNext(GossipResponse.newBuilder()
                    .setId(request.getId())
                    .setStatus(ResponseStatus.REJECTED)
                    .build());
            responseObserver.onCompleted();
            return;
        }

        responseObserver.onNext(GossipResponse.newBuilder()
                .setId(request.getId())
                .setData((Any) data.get().get())
                .setStatus(ResponseStatus.ACCEPTED)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void setData(GossipRequest request, StreamObserver<GossipResponse> responseObserver) {
        Long id = request.getId();
        K key = (K) request.getKey();
        T data = (T) request.getData();
        AtomicInteger count = new AtomicInteger(0);

        if (this.rpcDataHandler.setData(key, data, id)) {
            count.incrementAndGet();
        }

        Miscellaneous.copyShuffle(gosspyConfig.nodes().servers()).parallelStream().forEach((server) -> {
            if (count.get() >= gosspyConfig.nodes().writes()) {
                return;
            }
            GossipResponse response = sendGrpcRequest(request, server);
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
    }
}