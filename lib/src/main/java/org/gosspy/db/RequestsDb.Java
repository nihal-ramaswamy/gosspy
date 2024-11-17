package org.gosspy.db;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.gosspy.gen.protobuf.ResponseStatus;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Stores information of requests.
 */
@Slf4j
public class RequestsDb {

    @Getter
    private static final RequestsDb instance = new RequestsDb();

    /**
     * Private constructor to make class singleton.
     */
    private RequestsDb() {
    }


    /**
     * Inserts into REQUESTS_DB. If conflict, updates status.
     *
     * @param id      Request id.
     * @param nodeId  Node id.
     * @param status  Status.
     *
     * @throws SQLException Any exception during database operation.
     */
    public void upsertIntoRequests(Long id, Integer nodeId, ResponseStatus status) throws SQLException {
        String sql = "INSERT INTO REQUESTS_DB(ID, NODE_ID, STATUS) VALUES(?, ?, ?) ON CONFLICT(ID, NODE_ID) DO UPDATE SET STATUS = ? WHERE ID = ? AND NODE_ID = ?";
        Connection connection = ConnectionManager.getInstance();

        var statement = connection.prepareStatement(sql);
        statement.setLong(1, id);
        statement.setInt(2, nodeId);
        statement.setString(3, status.name());
        statement.setString(4, status.name());
        statement.setLong(5, id);
        statement.setInt(6, nodeId);

        statement.executeUpdate();
        statement.close();
    }

    /**
     * Returns status of request.
     * TODO: apply check to see if data exists.
     *
     * @param id      Request id.
     * @param nodeId  Node id.
     *
     * @return        Status.
     * @throws SQLException Any exception during database operation.
     */
    public ResponseStatus getRequestStatus(Long id, Integer nodeId) throws SQLException {
        String sql = "SELECT STATUS FROM REQUESTS_DB WHERE ID = ? AND NODE_ID = ?";

        Connection connection = ConnectionManager.getInstance();

        var statement = connection.prepareStatement(sql);
        statement.setLong(1, id);
        statement.setInt(2, nodeId);
        var resultSet = statement.executeQuery();

        if (resultSet.next()) {
            return ResponseStatus.valueOf(resultSet.getString("STATUS"));
        }

        resultSet.close();
        statement.close();

        return ResponseStatus.UNRECOGNIZED;
    }
}