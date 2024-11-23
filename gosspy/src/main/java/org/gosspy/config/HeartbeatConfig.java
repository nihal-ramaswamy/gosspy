package org.gosspy.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.net.URI;

/**
 * Heartbeat configuration.
 */
public record HeartbeatConfig(URI address, Long interval) {
}