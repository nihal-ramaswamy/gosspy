package org.gosspy.dto;

/**
 * Represents an abstract base class which holds the data to send through the gossip protocol.
 * Key = string, Type = string
 */
public interface Data {

 /**
  * Gets key from the object.
  *
  * @param data the data for which the key is required
  */
 String getKey(String data);

 /**
  * Gets the data assosciated with the given key.
  *
  * @param key the key to fetch data for
  * @return The value assosciated with the given key.
  */
 String getData(String key);

 /**
  * Sets the data assosciated with the given key.
  *
  * @param key  the key to set data for
  * @param data the data to set.
  * @return Whether or not the set was successful.
  */
 boolean setData(String key, String data);

 /**
  * Compares the two data and checks which one of them is newer.
  *
  * @param data1 the first data to compare
  * @param data2 the second data to compare
  */
 String getLatestVersion(String data1, String data2);
}