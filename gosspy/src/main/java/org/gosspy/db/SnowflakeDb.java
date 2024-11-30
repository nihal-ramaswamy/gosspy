package org.gosspy.db;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Manages database operations for snowflake db.
 */
@Slf4j
public class SnowflakeDb {
    /**
     * Singleton instance.
     */
    @Getter
    private static final SnowflakeDb instance = new SnowflakeDb();

    /**
     * Private constructor to make class singleton.
     */
    private SnowflakeDb() {

    }

    /**
     * Insert Node id, Counter into the database. Counter is set to 0.
     *
     * @param nodeId {@link int} node id
     * @throws SQLException Any exception when executing the query.
     */
    public void insertIntoSnowflakeWhereNodeIdIs(int nodeId) throws SQLException {
        log.atInfo().addKeyValue("nodeId", nodeId).addKeyValue("counter", 0).log("Adding row to SNOWFLAKE_COUNTER");
        Connection connection = ConnectionManager.getInstance();

        String sql = "INSERT INTO SNOWFLAKE_COUNTER(NODE_ID, COUNTER) VALUES (?, ?)";
        var statement = connection.prepareStatement(sql);
        statement.setInt(1, nodeId);
        statement.setInt(2, 0);
        statement.executeUpdate();
        statement.close();
    }

    /**
     * Increments the counter for the node id.
     * TODO: add a check to see if node exists
     *
     * @param nodeId {@link int} node id
     * @throws SQLException Any exception when executing the query.
     */
    public void incrementCounter(int nodeId) throws SQLException {
        log.atInfo().addKeyValue("nodeId", nodeId).log("Incrementing counter in SNOWFLAKE_COUNTER.");
        Connection connection = ConnectionManager.getInstance();
        String sql = "UPDATE SNOWFLAKE_COUNTER SET COUNTER = COUNTER + 1 WHERE NODE_ID = ?";
        var statement = connection.prepareStatement(sql);
        statement.setInt(1, nodeId);
        statement.executeUpdate();
        statement.close();

    }

    /**
     * Returns counter for the node id.
     *
     * @param nodeId {@link int} node id
     * @throws SQLException Any exception when executing the query.
     */
    public Integer selectCounter(int nodeId) throws SQLException {
        log.atInfo().addKeyValue("nodeId", nodeId).log("Fetching counter value in SNOWFLAKE_COUNTER.");

        int answer = 0;
        Connection connection = ConnectionManager.getInstance();
        String sql = "SELECT COUNTER FROM SNOWFLAKE_COUNTER WHERE NODE_ID = ?";
        var statement = connection.prepareStatement(sql);
        statement.setInt(1, nodeId);
        var resultSet = statement.executeQuery();

        while (resultSet.next()) {
            answer = resultSet.getInt("COUNTER");
        }

        resultSet.close();
        statement.close();


        return answer;
    }

    /**
     * Checks if node-counter exists in the database.
     *
     * @param nodeId {@link int} node id
     * @throws SQLException Any exception when executing the query.
     */
    public boolean nodeExists(int nodeId) throws SQLException {
        log.atInfo().addKeyValue("nodeId", nodeId).log("Checking if entry exists for node in SNOWFLAKE_COUNTER.");

        boolean answer;

        Connection connection = ConnectionManager.getInstance();
        String sql = "SELECT * FROM SNOWFLAKE_COUNTER WHERE NODE_ID = ?";
        var statement = connection.prepareStatement(sql);
        statement.setInt(1, nodeId);
        var resultSet = statement.executeQuery();
        answer = resultSet.next();
        resultSet.close();
        statement.close();

        return answer;
    }
}