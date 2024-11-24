package org.gosspy.utils;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;

public class Miscellaneous {
    public static <X> List<X> copyShuffle(List<X> items) {
        List<X> newItems = new java.util.ArrayList<>(items.stream().toList());
        Collections.shuffle(newItems);
        return newItems;
    }

    public static Boolean isElementInAtomicReferenceArray(AtomicReference<List<String>> atomicReferenceArray, String element) {
        return atomicReferenceArray.get().contains(element);
    }
}