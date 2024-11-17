package org.gosspy.config;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.net.URI;

/**
 * Stores curent server information.
 */
public record CurrentConfig(Integer id, URI address) {
}