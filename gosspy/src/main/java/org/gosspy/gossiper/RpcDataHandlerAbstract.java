package org.gosspy.gossiper;

import java.sql.SQLException;
import java.util.Optional;

public interface RpcDataHandlerAbstract<K, T> {
    Optional<T> getData(K key, Long id);

    boolean setData(K key, T data, Long id);
}