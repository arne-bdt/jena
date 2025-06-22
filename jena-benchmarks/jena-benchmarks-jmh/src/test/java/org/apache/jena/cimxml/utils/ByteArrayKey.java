package org.apache.jena.cimxml.utils;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A simple key class for byte arrays, using the first and last byte for hash code calculation.
 * This is a simplified version for demonstration purposes.
 */
public class ByteArrayKey implements SpecialByteBuffer {
    private final byte[] data;
    private final int length;
    private final int hashCode;
    private final String decodedString;


    public ByteArrayKey(String string) {
        var buffer = UTF_8.encode(string);
        this.data = buffer.array();
        this.length = buffer.limit();
        this.decodedString = string;
        this.hashCode = this.defaultHashCode();
    }

    public ByteArrayKey(final byte[] data) {
        this.data = data;
        this.length = data.length;
        this.hashCode = this.defaultHashCode();
        this.decodedString = new String(data, UTF_8);
    }

    public ByteArrayKey(final byte b) {
        this.data = new byte[]{b};
        this.length = 1;
        this.hashCode = b;
        this.decodedString = String.valueOf((char) b);
    }

    public byte[] getData() {
        return this.data;
    }

    @Override
    public int offset() {
        return 0;
    }

    @Override
    public int length() {
        return this.length;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SpecialByteBuffer otherBuffer) {
            return this.equals(otherBuffer);
        }
        return false;
    }

    @Override
    public String decodeToString() {
        return this.decodedString;
    }

    @Override
    public String toString() {
        return "ByteArrayKey [" + this.decodeToString() + "]";
    }
}
