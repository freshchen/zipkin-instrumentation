package com.github.freshchen.instrumentation.core.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author freshchen
 * @since 2022/3/1
 */
public class TagUtils {

    public static <V> String toString(Map<String, V> map) {
        List<String> list = new ArrayList<>();
        if (map != null) {
            for (Map.Entry<String, V> entry : map.entrySet()) {
                list.add(entry.getKey() + "=" + entry.getValue());
            }
        }
        return "{" + String.join(", ", list) + "}";
    }

    public static String toString(byte[][] array) {
        if (Objects.isNull(array)) {
            return "null";
        }

        List<String> list = new ArrayList<>();

        for (byte[] bytes : array) {
            list.add(Arrays.toString(bytes));
        }

        return "[" + String.join(", ", list) + "]";
    }

    public static String toString(Collection<byte[]> collection) {
        if (collection == null) {
            return "null";
        }
        List<String> list = new ArrayList<>();

        for (byte[] bytes : collection) {
            list.add(Arrays.toString(bytes));
        }

        return "[" + String.join(", ", list) + "]";
    }

    public static String toString(List<String> list) {
        if (list == null) {
            return "null";
        }

        return String.join(", ", list);
    }

    public static String toStringMap(Map<byte[], byte[]> map) {
        List<String> list = new ArrayList<>();
        if (map != null) {
            for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
                list.add(Arrays.toString(entry.getKey()) + "="
                    + Arrays.toString(entry.getValue()));
            }
        }

        return "{" + String.join(", ", list) + "}";

    }

    public static <V> String toStringMap2(Map<byte[], V> map) {
        List<String> list = new ArrayList<>();
        if (map != null) {
            for (Map.Entry<byte[], V> entry : map.entrySet()) {
                list.add(Arrays.toString(entry.getKey()) + "=" + entry.getValue());
            }
        }

        return "{" + String.join(", ", list) + "}";
    }

    public static String collectionToString(Collection<?> collection) {
        if (collection == null) {
            return "";
        }
        return collection.stream().map(Object::toString).collect(Collectors.joining(", "));
    }

    public static <K, V> String mapToString(Map<K, V> map) {
        if (map == null) {
            return "";
        }
        return map.entrySet()
            .stream()
            .map(entry -> entry.getKey() + " -> " + entry.getValue())
            .collect(Collectors.joining(", "));
    }


    private TagUtils() {

    }
}
