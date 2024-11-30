package org.gosspy.utils;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;

public class Miscellaneous {
    /**
     * Returns a shuffled list after performing a deepcopy
     *
     * @param <X> Generic contents of the list
     * @param items {@link List<X>} The list to be shuffled
     * @return {{@link List<X>} The shuffled list
     */
    public static <X> List<X> copyShuffle(List<X> items) {
        List<X> newItems = new java.util.ArrayList<>(items.stream().toList());
        Collections.shuffle(newItems);
        return newItems;
    }

    /**
     * Checks if the element is present inside a list of strings which is wrapped by an atomic reference.
     *
     * @param atomicReferenceArray {@link AtomicReference<List<String>>} The list of strings
     * @param element {@link String} The element to be searched
     * @return {@link Boolean} {@code true} if the element is found, else {@code false}
     */
    public static Boolean isElementInAtomicReferenceArray(AtomicReference<List<String>> atomicReferenceArray, String element) {
        return atomicReferenceArray.get().contains(element);
    }
}