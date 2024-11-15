package org.gosspy.db;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Manages database operations.
 */
@Slf4j
public class DatabaseManager {
    /**
     * Singleton instance.
     */
    @Getter
    private static final DatabaseManager instance = new DatabaseManager();

    /**
     * Private constructor to make class singleton.
     */
    private DatabaseManager() {

    }

    /**
     * Insert Node id, Counter into the database. Counter is set to 0.
     *
     * @param url    {@link String} url to the db
     * @param nodeId {@link int} node id
     * @throws SQLException Any exception when executing the query.
     */
    public void insertIntoSnowflakeWhereNodeIdIs(String url, int nodeId) throws SQLException {
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
     * TODO: add a check to see if node exists.
     * @throws SQLException Any exception when executing the query.
     * @param url {@link String} url to the db
     */
    public void incrementCounter(String url, int nodeId) throws SQLException {
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
     * @param url    {@link String} url to the db
     * @param nodeId {@link int} node id
     * @throws SQLException Any exception when executing the query.
     */
    public Integer selectCounter(String url, int nodeId) throws SQLException {
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
     * @param url    {@link String} url to the db
     * @param nodeId {@link int} node id
     * @throws SQLException Any exception when executing the query.
     */
    public boolean nodeExists(String url, int nodeId) throws SQLException {
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