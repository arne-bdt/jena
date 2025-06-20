package org.apache.jena.cimxml.utils;

public class WrappedByteArray implements SpecialByteBuffer {

    private final byte[] data;
    private final int offset;
    private final int length;
    private final int hashCode;

    public WrappedByteArray(byte[] data, int offset, int length) {
        this.data = data;
        this.offset = offset;
        this.length = length;
        this.hashCode = this.defaultHashCode();
    }

    @Override
    public int offset() {
        return offset;
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public byte[] getData() {
        return data;
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
    public String toString() {
        return "ReadonlyByteArrayBuffer [" + this.decodeToString() + "]";
    }
}
