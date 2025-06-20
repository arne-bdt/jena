package org.apache.jena.cimxml.utils;

public class AttributeFixedBuffer {
    private final QNameByteBuffer name;
    private final DecodingTextByteBuffer value;
    private boolean isConsumed = false;

    public AttributeFixedBuffer(QNameByteBuffer name, DecodingTextByteBuffer value) {
        this.name = name;
        this.value = value;
    }

    public QNameByteBuffer name() {
        return name;
    }

    public DecodingTextByteBuffer value() {
        return value;
    }

    public boolean isConsumed() {
        return isConsumed;
    }

    public void setConsumed() {
        isConsumed = true;
    }

    public void resetToUnconsumed() {
        isConsumed = false;
    }
}
