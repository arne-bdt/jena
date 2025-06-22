package org.apache.jena.cimxml.utils;

import org.apache.jena.cimxml.collections.JenaHashSet;

import java.util.ArrayList;
import java.util.List;

public class AttributeCollection {
    private final StreamBufferRoot streamingBufferRoot;
    private final List<AttributeFixedBuffer> attributeFixedBuffers = new ArrayList<>();
    private final JenaHashSet<Integer> alreadyConsumed = new JenaHashSet<>(16);
    private int currentAttributeIndex = -1;

    public AttributeCollection(StreamBufferRoot streamingBufferRoot) {
        this.streamingBufferRoot = streamingBufferRoot;
    }

    public void reset() {
        currentAttributeIndex = -1;
        alreadyConsumed.clear();
    }

    public QNameByteBuffer newAttribute() {
        final AttributeFixedBuffer buffer;
        currentAttributeIndex++;
        if (currentAttributeIndex == attributeFixedBuffers.size()) {
            buffer = new AttributeFixedBuffer(
                    new QNameByteBuffer(streamingBufferRoot),
                    new DecodingTextByteBuffer(streamingBufferRoot));
            attributeFixedBuffers.add(buffer);
        } else {
            buffer = attributeFixedBuffers.get(currentAttributeIndex);
            buffer.resetToUnconsumed();
        }
        return buffer.name();
    }

    public void discardCurrentAttribute() {
        currentAttributeIndex--;
    }

    public DecodingTextByteBuffer currentAttributeValue() {
        return attributeFixedBuffers.get(currentAttributeIndex).value();
    }

    public boolean isEmpty() {
        return currentAttributeIndex < 0;
    }

    public int size() {
        return currentAttributeIndex + 1;
    }

    public AttributeFixedBuffer get(int index) {
        return attributeFixedBuffers.get(index);
    }
}
