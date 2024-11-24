package org.gosspy.gossiper;

import java.util.Optional;

public interface RpcDataHandlerAbstract {
    Optional<String> getData(String key, Long id);

    Boolean setData(String key, String data, Long id);
}