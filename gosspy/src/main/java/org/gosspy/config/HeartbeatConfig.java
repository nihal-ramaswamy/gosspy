package org.gosspy.config;

import java.net.URI;

/**
 * Heartbeat configuration.
 */
public record HeartbeatConfig(URI address, Long interval) {
}