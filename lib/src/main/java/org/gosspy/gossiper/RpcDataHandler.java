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
public class RpcDataHandler<K, T> implements RpcDataHandlerAbstract<K, T> {

    private final Data<K, T> data;
    private final GosspyConfig gosspyConfig;

    private static final RequestsHistoryDb requestsDb = RequestsHistoryDb.getInstance();


    @Override
    public Optional<T> getData(K key, Long id) {
        try {
            if (RpcDataHandler.requestsDb.getRequestStatus(id, gosspyConfig.nodes().current().id()) == ResponseStatus.DUPLICATE) {
                return Optional.empty();
            }
        } catch (SQLException e) {
            log.error("Error in RPCDataHandler::getData: {}", e.getMessage());
        }

        return Optional.of(this.data.getData(key));
    }

    @Override
    public boolean setData(K key, T data, Long id) {
        try {
            if (RpcDataHandler.requestsDb.getRequestStatus(id, gosspyConfig.nodes().current().id()) == ResponseStatus.ACCEPTED) {
                return true;
            }
        } catch (SQLException e) {
            log.error("Error in RPCDataHandler::setData: {}", e.getMessage());
        }

        return this.data.setData(key, data);
    }
}