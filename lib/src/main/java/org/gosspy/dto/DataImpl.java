package org.gosspy.dto;

import org.apache.commons.math3.util.Pair;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class DataImpl implements Data<Integer, DataImpl.DataValue> {

    public record DataValue(Integer value, Timestamp timestamp) {
    }
    Map<Integer, DataValue> map = new HashMap<>();

    @Override
    public Integer getKey(DataValue data) {
        return
        this.map.keySet()
        .stream()
        .map(k -> Pair.create(k, this.map.get(k))).toList()
        .stream()
        .filter(p -> p.getValue().equals(data)).findFirst().get().getKey();
    }

    @Override
    public DataValue getData(Integer key) {
        return this.map.get(key);
    }

    @Override
    public boolean setData(Integer key, DataValue data) {
        this.map.put(key, data);
        return true;
    }

    @Override
    public DataValue getLatestVersion(DataValue data1, DataValue data2) {
        if (data1 == data2) {
            return data1;
        }
        if (data1.timestamp().after(data2.timestamp())) {
            return data1;
        } else {
            return data2;
        }
    }
}