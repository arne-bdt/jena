package org.apache.jena.cimxml.utils;

import com.github.jsonldjava.shaded.com.google.common.hash.Hashing;

import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.jena.cimxml.utils.ParserConstants.*;

public interface SpecialByteBuffer {
    // offset in the buffer where the data starts
    int offset();

    // Number of bytes in the buffer
    int length();

    // Returns the byte array containing the data
    byte[] getData();

    /**
     * Returns a hash code based on the first and last byte of the array.
     */
    default int defaultHashCode() {
        return Hashing.murmur3_32().hashBytes(getData(), offset(), length()).asInt();
    }

    default boolean equals(SpecialByteBuffer other) {
        if (this == other) return true;
        if (other == null) return false;

        if (this.length() != other.length()) return false;

        byte[] thisData = this.getData();
        byte[] otherData = other.getData();
        // Compare in reverse order, since in CIM XML, the last characters are more significant
        for (int i = this.length() - 1; i > -1; i--) {
            if (thisData[offset() + i] != otherData[other.offset() + i]) {
                return false; // Different content
            }
        }
        return true; // Same content
    }

    default java.nio.ByteBuffer wrapAsByteBuffer() {
        return java.nio.ByteBuffer.wrap(this.getData(), this.offset(), this.length());
    }

    default ByteArrayKey copy() {
        return new ByteArrayKey(this.copyToByteArray());
    }

    default byte[] joinedData(SpecialByteBuffer other) {
        if (other == null || other.length() == 0) {
            return this.copyToByteArray();
        }
        final byte[] combinedData = new byte[this.length() + other.length()];
        System.arraycopy(this.getData(), this.offset(), combinedData, 0, this.length());
        System.arraycopy(other.getData(), other.offset(), combinedData, this.length(), other.length());
        return combinedData;
    }

    default ByteArrayKey join(SpecialByteBuffer other) {
        return new ByteArrayKey(joinedData(other));
    }

    default String joinToString(SpecialByteBuffer other) {
        return UTF_8.decode(ByteBuffer.wrap(joinedData(other))).toString();
    }

    default ByteArrayKey join(SpecialByteBuffer... other) {
        if (other == null || other.length == 0) {
            return this.copy();
        }
        int totalLength = this.length();
        for (SpecialByteBuffer buf : other) {
            if (buf != null) {
                totalLength += buf.length();
            }
        }
        final byte[] combinedData = new byte[totalLength];
        System.arraycopy(this.getData(), this.offset(), combinedData, 0, this.length());
        int offset = this.length();
        for (SpecialByteBuffer buf : other) {
            if (buf != null && buf.length() > 0) {
                System.arraycopy(buf.getData(), buf.offset(), combinedData, offset, buf.length());
                offset += buf.length();
            }
        }
        return new ByteArrayKey(combinedData);
    }

    default byte[] copyToByteArray() {
        if (this.length() == 0) {
            return new byte[0];
        }
        final byte[] dataCopy = new byte[this.length()];
        System.arraycopy(this.getData(), this.offset(), dataCopy, 0, this.length());
        return dataCopy;
    }

    default String decodeToString() {
        return UTF_8.decode(this.wrapAsByteBuffer()).toString();
    }

    /// A heuristic to check if the content is probably a CIM uuid.
    /// These UUIDs start with an underscore or sharp and underscore
    ///  and are 37-38 characters long, with dashes at specific positions.
    /// Format: [#]_8-4-4-4-12 -> [#]_XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX
    default boolean isProbablyCimUuid() {
        // A very simple heuristic to check if the content is probably a UUID
        // This is not a strict check, just a quick way to filter out non-UUIDs
        if (this.length() == 38
                && this.getData()[this.offset()] == SHARP
                && this.getData()[this.offset() + 1] == UNDERSCORE
                && this.getData()[this.offset() + 10] == '-'
                && this.getData()[this.offset() + 15] == '-'
                && this.getData()[this.offset() + 20] == '-'
                && this.getData()[this.offset() + 25] == '-') {
            return true;
        }
        return this.length() == 37
                && this.getData()[this.offset()] == UNDERSCORE
                && this.getData()[this.offset() + 9] == '-'
                && this.getData()[this.offset() + 14] == '-'
                && this.getData()[this.offset() + 19] == '-'
                && this.getData()[this.offset() + 24] == '-';
    }
}
