package org.gosspy.utils;

import org.gosspy.db.ConnectionManager;

import java.sql.SQLException;

public class SnowflakeIdGenerator {
    public static Long getId(String url, Integer workerId) throws SQLException {
        ConnectionManager connectionManager = new ConnectionManager();
        boolean exists = connectionManager.nodeExists(url, workerId);
        if (!exists) {
            connectionManager.insertIntoSnowflakeWhereNodeIdIs(url, workerId);
        } else {
            connectionManager.incrementCounter(url, workerId);
        }

        long timestamp = System.currentTimeMillis();
        Long machineId = Long.valueOf(workerId);
        Long sequence = Long.valueOf(connectionManager.selectCounter(url, workerId));

        return timestamp << 22 | machineId << 16 | sequence;
    }
}