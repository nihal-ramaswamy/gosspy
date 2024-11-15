package org.gosspy.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Used to store data from {@link org.gosspy.constants.Constants.APPLICATION_YAML_FILE}.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class GosspyConfig {
    /**
     * Stores heartbeat configuration.
     */
    private HeartbeatConfig heartbeat;
}