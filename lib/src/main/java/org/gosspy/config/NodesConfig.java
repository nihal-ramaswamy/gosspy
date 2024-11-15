package org.gosspy.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.net.URI;
import java.util.List;

/**
 * Stores metadata of nodes.
 */
public record NodesConfig(Integer writes, Integer reads, List<URI> servers, CurrentConfig current) {

}