package org.gosspy.utils;

import org.gosspy.db.DatabaseManager;

import java.sql.SQLException;

/**
 * Class to generate snowflake id.
 */
public class SnowflakeIdGenerator {
    /**
     * Generates snowflake id.
     * It also increments the counter for that node in the database. If node does not exist, it adds it with a counter of 0.
     *
     * @param url {@link String} database url
     * @param workerId {@link Integer} node id
     */
    public static Long getId(String url, Integer workerId) throws SQLException {
        DatabaseManager databaseManager = DatabaseManager.getInstance();
        boolean exists = databaseManager.nodeExists(url, workerId);
        if (!exists) {
            databaseManager.insertIntoSnowflakeWhereNodeIdIs(url, workerId);
        } else {
            databaseManager.incrementCounter(url, workerId);
        }

        long timestamp = System.currentTimeMillis();
        Long machineId = Long.valueOf(workerId);
        Long sequence = Long.valueOf(databaseManager.selectCounter(url, workerId));

        return timestamp << 22 | machineId << 16 | sequence;
    }
}