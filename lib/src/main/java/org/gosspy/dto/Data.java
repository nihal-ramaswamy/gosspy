package org.gosspy.dto;

/**
 * Represents an abstract base class which holds the data to send through the gossip protocol.
 * K = key, T = data type
 */
public interface Data<K, T> {

 /**
  * Gets key from the object.
  *
  * @param data the data for which the key is required
  */
 K getKey(T data);

 /**
  * Gets the data assosciated with the given key.
  *
  * @param key the key to fetch data for
  * @return The value assosciated with the given key.
  */
 T getData(K key);

 /**
  * Sets the data assosciated with the given key.
  *
  * @param key  the key to set data for
  * @param data the data to set.
  * @return Whether or not the set was successful.
  */
 boolean setData(K key, T data);

 /**
  * Compares the two data and checks which one of them is newer.
  *
  * @param data1 the first data to compare
  * @param data2 the second data to compare
  */
 T getLatestVersion(T data1, T data2);
}