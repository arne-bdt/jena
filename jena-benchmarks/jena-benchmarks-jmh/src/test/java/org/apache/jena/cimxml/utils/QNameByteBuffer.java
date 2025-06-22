package org.apache.jena.cimxml.utils;

import org.apache.jena.cimxml.CIMParser;

import java.io.IOException;
import java.util.function.Consumer;

import static org.apache.jena.cimxml.utils.ParserConstants.*;

public class QNameByteBuffer extends StreamBufferChild {
    private int startOfLocalPart = 0; // Index where the local part starts

    public QNameByteBuffer(StreamBufferRoot parent) {
        super(parent);
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
        return new WrappedByteArray(root.buffer, start, startOfLocalPart - start - 1); // Exclude the colon
    }

    public WrappedByteArray getLocalPart() {
        return new WrappedByteArray(root.buffer, startOfLocalPart, endExclusive - startOfLocalPart);
    }

    @Override
    protected void afterConsumeCurrent() {
        if (root.buffer[root.position] == DOUBLE_COLON) {
            startOfLocalPart = root.position + 1; // Set the start of local part after the colon
        }
    }

    public boolean tryConsumeToEndOfTagName() throws IOException {
        if (root.position >= root.filledToExclusive) {
            if (!root.tryFillFromInputStream()) {
                return false; // No more data to read
            }
        }
        while (root.position < root.filledToExclusive) {
            if (isEndOfTagName(root.buffer[root.position])) {
                return true;
            }
            afterConsumeCurrent();
            if (++root.position == root.filledToExclusive) {
                if (!root.tryFillFromInputStream()) {
                    return false; // No more data to read
                }
            }
        }
        return false; // Byte not found
    }

    public boolean tryConsumeUntilNonWhitespace() throws IOException {
        if (root.position >= root.filledToExclusive) {
            if (!root.tryFillFromInputStream()) {
                return false; // No more data to read
            }
        }
        while (root.position < root.filledToExclusive) {
            if (!isWhitespace(root.buffer[root.position])) {
                // no need to call afterConsumeCurrent here, as we are skipping whitespace
                return true;
            }
            afterConsumeCurrent(); // Consume the current byte
            if (++root.position == root.filledToExclusive) {
                if (!root.tryFillFromInputStream()) {
                    return false; // No more data to read
                }
            }
        }
        return false; // Byte not found
    }

    /**
     * Tries to consume until the end of the QName, which is defined as
     * the first whitespace or the equality sign ('=').
     * @return
     * @throws IOException
     */
    public boolean tryConsumeUntilEndOfQName() throws IOException {
        if (root.position >= root.filledToExclusive) {
            if (!root.tryFillFromInputStream()) {
                return false; // No more data to read
            }
        }
        while (root.position < root.filledToExclusive) {
            if (isWhitespace(root.buffer[root.position]) || root.buffer[root.position] == EQUALITY_SIGN) {
                // no need to call afterConsumeCurrent here, as we are skipping whitespace and equality sign
                return true;
            }
            afterConsumeCurrent();
            if (++root.position == root.filledToExclusive) {
                if (!root.tryFillFromInputStream()) {
                    return false; // No more data to read
                }
            }
        }
        return false; // Byte not found
    }
}
