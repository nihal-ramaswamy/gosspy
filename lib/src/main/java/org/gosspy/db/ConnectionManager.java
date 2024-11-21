package org.gosspy.db;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Manages the connection to db.
 */
@Slf4j
public class ConnectionManager {
    /**
     * Singleton instance
     */
    @Getter
    private static Connection connection = null;

    /**
     * Private constructor to make class singleton.
     */
    private ConnectionManager() {

    }

    /**
     * Initializes the connection with given url. This should be called only once and in the beginning of the program.
     * Make sure to close the connection before exiting the program.
     *
     * @param url {@link String} URL to the db
     * @throws SQLException if there is an error in the connection
     * @throws RuntimeException if init is called more than once
     */
    public static void init(String url) throws SQLException, RuntimeException {
        if (connection != null) {
            throw new RuntimeException("Connection already instantiated");
        }
        connection = DriverManager.getConnection(url);
    }

    /**
     * Returns the instance of connection.
     */
    public static Connection getInstance() {
        return connection;
    }

    /**
     * Closes the connection. To be called at the end of the program.
     */
    public static void close()  {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.atError().addKeyValue("message", e.getMessage()).log();
            }
        }
    }
}