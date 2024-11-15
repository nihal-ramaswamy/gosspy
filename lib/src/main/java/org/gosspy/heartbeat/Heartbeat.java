package org.gosspy.heartbeat;

import com.google.gson.Gson;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Sends heartbeats to a preconfigured URI.
 */
@Slf4j
public class Heartbeat {

    /**
     * Message format: {"message": "PING"}
     */
    private static final String message = new Gson().toJson(new Heartbeat.PingMessage());

    /**
     * Class used to generate the JSON message to be sent as heartbeat.
     */
    @Getter
    private static class PingMessage {
        /**
         * The message to be sent.
         */
        private final String message = "PING";
    }

    /**
     * Heartbeat implementation which sends a post request to specified URI.
     * Message format:
     * {
     * "message": "PING"
     * }
     *
     * @param uri        {@link URI} The URI to hit.
     */
    private static void heartbeat(URI uri) {
        try (HttpClient httpClient = HttpClient.newHttpClient()) {
            try {
                log.info("Sending heartbeat...");

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(uri)
                        .timeout(Duration.of(10, java.time.temporal.ChronoUnit.SECONDS))
                        .POST(HttpRequest.BodyPublishers.ofString(message))
                        .build();

                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() != HttpURLConnection.HTTP_OK) {
                    log.info("Heartbeat status failed with code: {}", response.statusCode());
                }

            } catch (Exception e) {
                log.error("Error sending heartbeat: {}", e.getMessage());
            }
        } catch (Exception e) {
            log.error("Error starting heartbeat: {}", e.getMessage());
        }
    }

    /**
     * Sends heartbeats in a loop.
     *
     * @param uri              {@link URI} URI to send heartbeats to.
     * @param intervalInMillis {@link Long} How often to send heartbeats in milliseconds.
     */
    public void start(URI uri, Long intervalInMillis) throws RuntimeException {
        Timer timer = new Timer();

        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                Heartbeat.heartbeat(uri);
            }
        };
        timer.scheduleAtFixedRate(task, 0, intervalInMillis);
    }
}