package org.gosspy.config;

/**
 * Used to store data from {@code org.gosspy.constants.Constants.APPLICATION_YAML_FILE}.
 */
public record GosspyConfig(HeartbeatConfig heartbeat, NodesConfig nodes, SnowflakeConfig snowflake) {
}