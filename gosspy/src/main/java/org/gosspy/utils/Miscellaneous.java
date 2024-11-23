package org.gosspy.utils;

import java.util.Collections;
import java.util.List;

public class Miscellaneous {
    public static <X> List<X> copyShuffle(List<X> items) {
        List<X> newItems = new java.util.ArrayList<>(items.stream().toList());
        Collections.shuffle(newItems);
        return newItems;
    }
}