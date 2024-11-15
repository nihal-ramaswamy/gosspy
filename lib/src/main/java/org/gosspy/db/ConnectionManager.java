package org.gosspy.db;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Slf4j
public class ConnectionManager {
    private static Connection getConnection(String url) throws SQLException {
        return DriverManager.getConnection(url);
    }

    public void insertIntoSnowflakeWhereNodeIdIs(String url, int nodeId) throws SQLException {
        String sql = "INSERT INTO SNOWFLAKE_COUNTER(NODE_ID, COUNTER) VALUES (?, ?)";
        Connection connection = ConnectionManager.getConnection(url);
        var statement = connection.prepareStatement(sql);
        statement.setInt(1, nodeId);
        statement.setInt(2, 0);
        statement.executeUpdate();
        statement.close();
        connection.close();
    }

    public void incrementCounter(String url, int nodeId) throws SQLException {
        String sql = "UPDATE SNOWFLAKE_COUNTER SET COUNTER = COUNTER + 1 WHERE NODE_ID = ?";
        Connection connection = ConnectionManager.getConnection(url);
        var statement = connection.prepareStatement(sql);
        statement.setInt(1, nodeId);
        statement.executeUpdate();
        statement.close();
    }

    public Integer selectCounter(String url, int nodeId) throws SQLException {
        String sql = "SELECT COUNTER FROM SNOWFLAKE_COUNTER WHERE NODE_ID = ?";
        Connection connection = ConnectionManager.getConnection(url);
        var statement = connection.prepareStatement(sql);
        statement.setInt(1, nodeId);
        var resultSet = statement.executeQuery();

        int answer = 0;
        while (resultSet.next()) {
            answer = resultSet.getInt("COUNTER");
        }

        resultSet.close();
        statement.close();

        return answer;
    }

    public boolean nodeExists(String url, int nodeId) throws SQLException {
        String sql = "SELECT * FROM SNOWFLAKE_COUNTER WHERE NODE_ID = ?";
        Connection connection = ConnectionManager.getConnection(url);
        var statement = connection.prepareStatement(sql);
        statement.setInt(1, nodeId);
        var resultSet = statement.executeQuery();
        boolean answer = resultSet.next();
        resultSet.close();
        statement.close();
        return answer;
    }
}