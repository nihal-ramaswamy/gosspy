/*
 * This source file was generated by the Gradle 'init' task
 */
package org.gosspy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.extern.slf4j.Slf4j;
import org.gosspy.config.GosspyConfig;
import org.gosspy.constants.Constants;
import org.gosspy.db.ConnectionManager;
import org.gosspy.heartbeat.Heartbeat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.sql.SQLException;

@Slf4j
public class Gosspy {
    /**
     * Main function which sets up gosspy.
     */
    public void run() throws IOException, SQLException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.findAndRegisterModules();
        URL fileUrl = getClass().getClassLoader().getResource(Constants.APPLICATION_YAML_FILE);
        if (fileUrl == null) {
            throw new FileNotFoundException("application.yml not found in resources");
        }
        GosspyConfig gosspyConfig = mapper.readValue(fileUrl, GosspyConfig.class);

        log.info(gosspyConfig.toString());

        Thread thread = new Thread(() -> {
            Heartbeat heartbeat = new Heartbeat();
            heartbeat.start(gosspyConfig.heartbeat().address(), gosspyConfig.heartbeat().interval());
        });

        thread.setName("heartbeat");
        thread.start();

        ConnectionManager.init(gosspyConfig.snowflake().database());
    }

    /**
     * Used for testing
     */
    public static void main(String[] args) {
        try {
            new Gosspy().run();
        } catch (Exception e) {
            ConnectionManager.close();
            log.error(e.getMessage());
        }
    }
}
