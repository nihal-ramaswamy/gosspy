package org.gosspy.dto;

/**
 * Represents an abstract base class which holds the data to send through the gossip protocol.
 */
public abstract class AbstractData<K, T> {
    /**
     * Gets the data assosciated with the given key.
     *
     * @param key the key to fetch data for
     * @return The value assosciated with the given key.
     */
    public abstract T getData(K key);

    /**
     * Sets the data assosciated with the given key.
     *
     * @param key the key to set data for
     * @param data the data to set.
     *
     * @return Whether or not the set was successful.
     */
    public abstract boolean setData(K key, T data);

    /**
     * Compares the two data and checks which one of them is newer.
     *
     * @param data1 the first data to compare
     * @param data2 the second data to compare
     */
    public abstract T getLatestVersion(T data1, T data2);
}