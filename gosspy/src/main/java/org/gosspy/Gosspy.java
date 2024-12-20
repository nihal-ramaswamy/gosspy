/*
 * This source file was generated by the Gradle 'init' task
 */
package org.gosspy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import lombok.extern.slf4j.Slf4j;
import org.gosspy.config.GosspyConfig;
import org.gosspy.constants.Constants;
import org.gosspy.db.ConnectionManager;
import org.gosspy.dto.Data;
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
     *
     * @param data {@link Data} The data class that is sent across the grpc protocol
     */
    public void run(Data data) throws IOException, SQLException {
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

        log.atInfo().addKeyValue("databaseUrl", gosspyConfig.snowflake().database()).log("Connecting to database");
        ConnectionManager.init(gosspyConfig.snowflake().database());
        log.atInfo().addKeyValue("connectionClosed", ConnectionManager.getConnection().getClientInfo()).log("testing connection");

        URI currentAddress = gosspyConfig.nodes().current().address();

        RpcDataHandler rpcDataHandler = new RpcDataHandler(data, gosspyConfig);
        RpcNetworkHandler networkHandler = new RpcNetworkHandler(data, rpcDataHandler, gosspyConfig);
        HealthStatusManager health = new HealthStatusManager();

        // TODO: Make this secure
        final Server server = ServerBuilder.forPort(currentAddress.getPort())
                .addService(networkHandler)
                .addService(health.getHealthService())
                .addService(ProtoReflectionServiceV1.newInstance())
                .build().start();

        log.atInfo().addKeyValue("port", currentAddress.getPort()).log("Server listening");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
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
                ConnectionManager.close();
                server.shutdownNow();
                log.atInfo().log("Shutting down server");
            }
        }));
        health.setStatus("", HealthCheckResponse.ServingStatus.SERVING);
    }
}
