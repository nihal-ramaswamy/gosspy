package org.gosspy.config;


import java.net.URI;

/**
 * Stores curent server information.
 */
public record CurrentConfig(Integer id, URI address) {
}