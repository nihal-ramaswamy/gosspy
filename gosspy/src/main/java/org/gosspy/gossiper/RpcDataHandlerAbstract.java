package org.gosspy.gossiper;

import java.sql.SQLException;
import java.util.Optional;

public interface RpcDataHandlerAbstract {
    Optional<String> getData(String key, Long id);

    boolean setData(String key, String data, Long id);
}