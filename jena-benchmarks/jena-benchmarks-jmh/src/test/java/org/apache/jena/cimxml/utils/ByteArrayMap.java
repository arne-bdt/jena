package org.apache.jena.cimxml.utils;

import org.apache.jena.cimxml.collections.JenaHashMap;

import java.util.function.Supplier;

public class ByteArrayMap<V> {
    private final int expectedMaxEntriesWithSameLength;
    private JenaHashMap<SpecialByteBuffer, V>[] entriesWithSameLength;

    public ByteArrayMap(int expectedMaxByteLength, int expectedEntriesWithSameLength) {
        var positionsSize = Integer.highestOneBit(expectedMaxByteLength << 1);
        if (positionsSize < expectedMaxByteLength << 1) {
            positionsSize <<= 1;
        }
        this.entriesWithSameLength = new JenaHashMap[positionsSize];
        this.expectedMaxEntriesWithSameLength = expectedEntriesWithSameLength;
    }

    private void grow(final int minimumLength) {
        var newLength = entriesWithSameLength.length << 1;
        while (newLength < minimumLength) {
            newLength = minimumLength << 1;
        }
        final var oldValues = entriesWithSameLength;
        entriesWithSameLength = new JenaHashMap[newLength];
        System.arraycopy(oldValues, 0, entriesWithSameLength, 0, oldValues.length);
    }

    public void put(ByteArrayKey key, V value) {
        final JenaHashMap<SpecialByteBuffer, V> map;
        // Ensure the array is large enough
        if (entriesWithSameLength.length < key.length()) {
            grow(key.length());
            map = new JenaHashMap<>(expectedMaxEntriesWithSameLength);
            entriesWithSameLength[key.length()] = map;
            map.put(key, value);
            return;
        }
        if (entriesWithSameLength[key.length()] == null) {
            map = new JenaHashMap<>(expectedMaxEntriesWithSameLength);
            entriesWithSameLength[key.length()] = map;
        } else {
            map = entriesWithSameLength[key.length()];
        }
        map.put(key, value);
    }

    public boolean tryPut(ByteArrayKey key, V value) {
        final JenaHashMap<SpecialByteBuffer, V> map;
        // Ensure the array is large enough
        if (entriesWithSameLength.length < key.length()) {
            grow(key.length());
            map = new JenaHashMap<>(expectedMaxEntriesWithSameLength);
            entriesWithSameLength[key.length()] = map;
            map.put(key, value);
            return true;
        }
        if (entriesWithSameLength[key.length()] == null) {
            map = new JenaHashMap<>(expectedMaxEntriesWithSameLength);
            entriesWithSameLength[key.length()] = map;
        } else {
            map = entriesWithSameLength[key.length()];
        }
        return map.tryPut(key, value);
    }

    public V computeIfAbsent(ByteArrayKey key, Supplier<V> mappingFunction) {
        final JenaHashMap<SpecialByteBuffer, V> map;
        // Ensure the array is large enough
        if (entriesWithSameLength.length < key.length()) {
            grow(key.length());
            map = new JenaHashMap<>(expectedMaxEntriesWithSameLength);
            entriesWithSameLength[key.length()] = map;
            final var value = mappingFunction.get();
            map.put(key, value);
            return value;
        }
        if (entriesWithSameLength[key.length()] == null) {
            map = new JenaHashMap<>(expectedMaxEntriesWithSameLength);
            entriesWithSameLength[key.length()] = map;
        } else {
            map = entriesWithSameLength[key.length()];
        }
        return map.computeIfAbsent(key, mappingFunction);
    }

    public V get(SpecialByteBuffer key) {
        if (entriesWithSameLength.length < key.length() || entriesWithSameLength[key.length()] == null) {
            return null;
        }
        return entriesWithSameLength[key.length()].get(key);
    }

    public boolean containsKey(ByteArrayKey key) {
        if (entriesWithSameLength.length < key.length() || entriesWithSameLength[key.length()] == null) {
            return false;
        }
        return entriesWithSameLength[key.length()].containsKey(key);
    }

}
