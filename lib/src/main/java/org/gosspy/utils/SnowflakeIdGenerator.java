package org.gosspy.utils;

import org.gosspy.db.SnowflakeDb;

import java.sql.SQLException;

/**
 * Class to generate snowflake id.
 */
public class SnowflakeIdGenerator {
    /**
     * Generates snowflake id.
     * It also increments the counter for that node in the database. If node does not exist, it adds it with a counter of 0.
     *
     * @param workerId {@link Integer} node id
     */
    public static Long getId(Integer workerId) throws SQLException {
        SnowflakeDb databaseManager = SnowflakeDb.getInstance();
        boolean exists = databaseManager.nodeExists(workerId);
        if (!exists) {
            databaseManager.insertIntoSnowflakeWhereNodeIdIs(workerId);
        } else {
            databaseManager.incrementCounter(workerId);
        }

        long timestamp = System.currentTimeMillis(); // 41 bits
        timestamp %= (1L << 41);

        Long machineId = Long.valueOf(workerId); // 12 bits
        machineId %= (1L << 12);

        Long sequence = Long.valueOf(databaseManager.selectCounter(workerId)); // 10 bits
        sequence %= (1L << 10); // 1024

        return timestamp | machineId| sequence;
    }
}