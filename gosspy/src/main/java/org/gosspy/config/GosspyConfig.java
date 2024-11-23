package org.gosspy.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.gosspy.heartbeat.Heartbeat;

/**
 * Used to store data from {@link org.gosspy.constants.Constants.APPLICATION_YAML_FILE}.
 */
public record GosspyConfig(HeartbeatConfig heartbeat, NodesConfig nodes, SnowflakeConfig snowflake) {
}