package org.apache.jena.cimxml.utils;

import org.apache.jena.cimxml.CIMParser;

import java.io.IOException;
import java.util.function.Consumer;

import static org.apache.jena.cimxml.utils.ParserConstants.DOUBLE_COLON;

public class QNameByteBuffer extends StreamBufferChild {
    private int startOfLocalPart = 0; // Index where the local part starts

    public QNameByteBuffer(StreamBufferRoot parent, int size) {
        super(parent, size);
    }

    @Override
    public void reset() {
        super.reset();
        this.startOfLocalPart = 0;
    }

    public boolean hasPrefix() {
        return startOfLocalPart != 0; // If local part starts after the first byte, it has a prefix
    }

    public WrappedByteArray getPrefix() {
        return new WrappedByteArray(buffer, start, startOfLocalPart - start - 1); // Exclude the colon
    }

    public WrappedByteArray getLocalPart() {
        return new WrappedByteArray(buffer, startOfLocalPart, endExclusive - startOfLocalPart);
    }

    @Override
    protected void afterConsumeCurrent() {
        if (buffer[position] == DOUBLE_COLON) {
            startOfLocalPart = position + 1; // Set the start of local part after the colon
        }
    }
}
