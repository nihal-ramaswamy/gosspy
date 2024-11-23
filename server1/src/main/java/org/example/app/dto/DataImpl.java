package org.example.app.dto;

import org.apache.commons.math3.util.Pair;
import org.gosspy.dto.Data;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class DataImpl implements Data {
    Map<String, String> map = new HashMap<>();

    @Override
    public String getKey(String data) {
        return
        this.map.keySet()
        .stream()
        .map(k -> Pair.create(k, this.map.get(k))).toList()
        .stream()
        .filter(p -> p.getValue().equals(data)).findFirst().get().getKey();
    }

    @Override
    public String getData(String key) {
        return this.map.get(key);
    }

    @Override
    public boolean setData(String key, String data) {
        this.map.put(key, data);
        return true;
    }

    @Override
    public String getLatestVersion(String data1, String data2) {
        return data1;
    }
}