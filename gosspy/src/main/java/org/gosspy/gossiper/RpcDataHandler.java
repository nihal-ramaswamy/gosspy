package org.gosspy.gossiper;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gosspy.config.GosspyConfig;
import org.gosspy.db.RequestsHistoryDb;
import org.gosspy.dto.Data;
import org.gosspy.gen.protobuf.ResponseStatus;

import java.sql.SQLException;
import java.util.Optional;


@AllArgsConstructor
@Slf4j
public class RpcDataHandler implements RpcDataHandlerAbstract {

    private final Data data;
    private final GosspyConfig gosspyConfig;

    private static final RequestsHistoryDb requestsDb = RequestsHistoryDb.getInstance();

    /**
     * Contains the logic to fetch the data from the database
     *
     * @param key {@link String} The key for which the data is required
     * @param id {@link Long} The request id
     * @return {@link Optional<String>} Returns the data if found
     */
    @Override
    public Optional<String> getData(String key, Long id) {
        try {
            if (RpcDataHandler.requestsDb.getRequestStatus(id, gosspyConfig.nodes().current().id()) == ResponseStatus.DUPLICATE) {
                return Optional.empty();
            }
        } catch (SQLException e) {
            log.atError().addKeyValue("message", e.getMessage()).log();
        }

        try {
            RpcDataHandler.requestsDb.upsertIntoRequests(id, gosspyConfig.nodes().current().id(), ResponseStatus.ACCEPTED);
        } catch (SQLException e) {
            log.atError().addKeyValue("message", e.getMessage()).log();
        }

        return Optional.of(this.data.getData(key));
    }

    /**
     * Sets the data in the database
     *
     * @param key {@link String} The key for which the data is set with
     * @param data {@link String} The data itself
     * @param id {@link Long} The request id
     * @return {@link Boolean} Returns true if the data is set
     */
    @Override
    public Boolean setData(String key, String data, Long id) {
        try {
            if (RpcDataHandler.requestsDb.getRequestStatus(id, gosspyConfig.nodes().current().id()) == ResponseStatus.ACCEPTED) {
                return true;
            }
        } catch (SQLException e) {
            log.atError().addKeyValue("message", e.getMessage()).log();
        }

        try {
            RpcDataHandler.requestsDb.upsertIntoRequests(id, gosspyConfig.nodes().current().id(), ResponseStatus.ACCEPTED);
        } catch (SQLException e) {
            log.atError().addKeyValue("message", e.getMessage()).log();
        }

        return this.data.setData(key, data);
    }
}