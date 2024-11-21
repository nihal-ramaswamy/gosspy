/*
 * This source file was generated by the Gradle 'init' task
 */
package org.gosspy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.protobuf.services.HealthStatusManager;
import lombok.extern.slf4j.Slf4j;
import org.gosspy.config.GosspyConfig;
import org.gosspy.constants.Constants;
import org.gosspy.db.ConnectionManager;
import org.gosspy.dto.DataImpl;
import org.gosspy.gossiper.RpcDataHandler;
import org.gosspy.gossiper.RpcNetworkHandler;
import org.gosspy.heartbeat.Heartbeat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Gosspy {
    /**
     * Main function which sets up gosspy.
     */
    public void run() throws IOException, SQLException, InterruptedException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.findAndRegisterModules();
        URL fileUrl = getClass().getClassLoader().getResource(Constants.APPLICATION_YAML_FILE);
        if (fileUrl == null) {
            throw new FileNotFoundException("application.yml not found in resources");
        }
        GosspyConfig gosspyConfig = mapper.readValue(fileUrl, GosspyConfig.class);

        log.atInfo().addKeyValue("gosspyConfig", gosspyConfig).log();

        Thread thread = new Thread(() -> {
            Heartbeat heartbeat = new Heartbeat();
            heartbeat.start(gosspyConfig.heartbeat().address(), gosspyConfig.heartbeat().interval());
        });

        thread.setName("heartbeat");
        thread.start();

        ConnectionManager.init(gosspyConfig.snowflake().database());

        URI currentAddress = gosspyConfig.nodes().current().address();
        DataImpl data = new DataImpl();
        RpcDataHandler<Integer, DataImpl.DataValue> rpcDataHandler = new RpcDataHandler<>(data, gosspyConfig);
        RpcNetworkHandler<Integer, DataImpl.DataValue> networkHandler = new RpcNetworkHandler<>(data, rpcDataHandler, gosspyConfig);
        HealthStatusManager health = new HealthStatusManager();
        final Server server = Grpc.newServerBuilderForPort(currentAddress.getPort(), InsecureServerCredentials.create())
                .addService(networkHandler)
                .addService(health.getHealthService())
                .build().start();

        log.atInfo().addKeyValue("port", currentAddress.getPort()).log("Server listening");

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Start graceful shutdown
                server.shutdown();
                try {
                    // Wait for RPCs to complete processing
                    if (!server.awaitTermination(30, TimeUnit.SECONDS)) {
                        // That was plenty of time. Let's cancel the remaining RPCs
                        server.shutdownNow();
                        // shutdownNow isn't instantaneous, so give a bit of time to clean resources up
                        // gracefully. Normally this will be well under a second.
                        server.awaitTermination(5, TimeUnit.SECONDS);
                    }
                } catch (InterruptedException ex) {
                    server.shutdownNow();
                    log.atInfo().log("Shutting down server");
                }
            }
        });
        health.setStatus("", HealthCheckResponse.ServingStatus.SERVING);
        server.awaitTermination();
    }

    /**
     * Used for testing
     */
    public static void main(String[] args) {
        try {
            new Gosspy().run();
        } catch (Exception e) {
            ConnectionManager.close();
            log.atError().addKeyValue("message", e.getMessage()).log();
        }
    }
}
