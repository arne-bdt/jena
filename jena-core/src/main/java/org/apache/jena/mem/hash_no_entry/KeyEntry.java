package org.apache.jena.mem.hash_no_entry;

public class KeyEntry<T> {
    final int hash;
    ValueMap<T> value;
    KeyEntry<T> next;

    public KeyEntry(int hash, ValueMap<T> value) {
        this.hash = hash;
        this.value = value;
    }
}
