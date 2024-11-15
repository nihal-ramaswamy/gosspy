package org.gosspy.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.net.URI;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class HeartbeatConfig {
    private URI address;
    private Long interval;
}