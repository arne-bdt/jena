package org.apache.jena.cimxml.utils;

import java.io.IOException;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.jena.cimxml.utils.ParserConstants.END_OF_STREAM;

public abstract class StreamBufferChild implements SpecialByteBuffer {
    /**
     * The root buffer that this child belongs to.
     * This is used to access the input stream for reading data.
     * It also holds the last used buffer, which is used to handover remaining bytes.
     */
    protected final StreamBufferRoot root;
    /**
     * The byte array buffer that holds the data read from the input stream.
     */
    protected final byte[] buffer;
    /**
     * The offset in the buffer where the data starts.
     */
    protected int start = 0;
    /**
     * Marks the end of relevant data in the buffer.
     */
    protected int endExclusive = 0;
    /**
     * This marks the position to which the buffer is filled.
     */
    protected int filledToExclusive = 0;

    /**
     * The position in the buffer where the next byte will be read.
     */
    protected int position = 0;

    protected boolean abort = false;

    public StreamBufferChild(StreamBufferRoot parent, int size) {
        if (parent == null) {
            throw new IllegalArgumentException("Parent buffer cannot be null");
        }
        this.root = parent;
        this.buffer = new byte[size];
    }

    public void reset() {
        this.start = 0;
        this.endExclusive = 0;
        this.filledToExclusive = 0;
        this.position = 0;
    }

    public void abort() {
        this.abort = true;
    }

    public void setCurrentByteAsStartPositon() {
        this.start = this.position;
    }

    public void setNextByteAsStartPositon() {
        this.start = this.position + 1;
    }

    public void setEndPositionExclusive() {
        this.endExclusive = this.position;
    }

    public boolean hasRemainingCapacity() {
        return filledToExclusive < buffer.length;
    }

    public boolean tryForwardAndSetStartPositionAfter(byte byteToSeek) throws IOException {
        if(tryForwardToByte(byteToSeek)) {
            setNextByteAsStartPositon();
            return  true;
        }
        return false;
    }

    public boolean tryForwardAndSetEndPositionExclusive(byte byteToSeek) throws IOException {
        if(tryForwardToByte(byteToSeek)) {
            setEndPositionExclusive();
            return  true;
        }
        return false;
    }

    protected abstract void afterConsumeCurrent();

//    public boolean tryForwardToByte(byte byteToSeek) throws IOException {
//        var abortBefore = this.abort; // memoize the current abort state to avoid side effects
//        boolean[] found = {false};
//        this.consumeBytes(b -> {
//            if (b == byteToSeek) {
//                abort();
//                found[0] = true;
//            }
//        });
//        if (abortBefore) {
//            this.abort = true; // Restore the abort state if it was set before
//        }
//        return found[0];
//    }

    public boolean tryForwardToByte(byte byteToSeek) throws IOException {
        if (position >= filledToExclusive) {
            if (!tryFillFromInputStream()) {
                return false; // No more data to read
            }
        }
        while (position < filledToExclusive) {
            if (buffer[position] == byteToSeek) {
                return true;
            }
            afterConsumeCurrent();
            if (++position == filledToExclusive) {
                if (!tryFillFromInputStream()) {
                    return false; // No more data to read
                }
            }
        }
        return false; // Byte not found
    }

    public boolean tryForwardToByteAfter(byte byteToSeek) throws IOException {
        boolean found = tryForwardToByte(byteToSeek);
        position++;
        return found;
    }

    /**
     * Copies remaining bytes from the last used child buffer of the parent.
     * This is used to handover remaining bytes from one child buffer to the next.
     * Attention: The predecessor may be identical to this child buffer.
     * In that case, the remaining bytes are copied to the beginning of this buffer
     */
    public void copyRemainingBytesFromPredecessor() {
        if (root.lastUsedChildBuffer == null) {
            root.lastUsedChildBuffer = this;
            reset();
            return; // Nothing to copy
        }
        var predecessor = root.lastUsedChildBuffer;
        var remainingBytes = predecessor.filledToExclusive - predecessor.position;
        if (remainingBytes == 0) {
            root.lastUsedChildBuffer = this;
            reset();
            return; // No remaining bytes to copy
        }
        System.arraycopy(predecessor.buffer, predecessor.position,
                this.buffer, 0, remainingBytes);
        reset();
        this.filledToExclusive = remainingBytes;
        root.lastUsedChildBuffer = this;
    }

    public byte peek() throws IOException {
        if (position >= filledToExclusive) {
            if (!tryFillFromInputStream()) {
                return END_OF_STREAM;
            }
        }
        return buffer[position];
    }

    /**
     * Reads the next byte from the buffer and advances the position.
     *
     * @return the next byte in the buffer
     * @throws IOException if an I/O error occurs while reading from the input stream
     */
    public byte next() throws IOException {
        position++;
        return peek();
    }

    /**
     * Skips the current byte and moves to the next one.
     * This does not change the start or end positions.
     *
     * @throws IOException if an I/O error occurs while reading from the input stream
     */
    public void skip() throws IOException {
        position++;
        peek();
    }

    public void consumeBytes(Consumer<Byte> byteConsumer) throws IOException {
        var abortBefore = this.abort; // memorize the current abort state to avoid side effects
        abort = false;
        if (position >= filledToExclusive) {
            if (!tryFillFromInputStream()) {
                byteConsumer.accept(END_OF_STREAM);
                return; // No more data to read
            }
        }
        while (position < filledToExclusive) {
            byteConsumer.accept(buffer[position]);
            if (abort) {
                return;
            }
            afterConsumeCurrent();
            if (++position >= filledToExclusive) {
                if (!tryFillFromInputStream()) {
                    byteConsumer.accept(END_OF_STREAM);
                    return; // No more data to read
                }
            }
        }
        byteConsumer.accept(END_OF_STREAM);
        if (abortBefore) {
            this.abort = true; // Restore the abort state if it was set before
        }
    }

    private boolean tryFillFromInputStream() throws IOException {
        if (hasRemainingCapacity()) {
            var bytesRead = root.inputStream.read(this.buffer, filledToExclusive,
                    buffer.length - filledToExclusive);
            if (bytesRead == -1) {
                return false;
            }
            filledToExclusive += bytesRead;
            return true;
        }
        return false;
    }

    @Override
    public int offset() {
        return start;
    }

    @Override
    public int length() {
        return endExclusive - start;
    }

    @Override
    public byte[] getData() {
        return this.buffer;
    }

    @Override
    public String toString() {
        String text;
        if (start == 0 && endExclusive == 0) {
            text = "Start at 0:[" +
                    UTF_8.decode(java.nio.ByteBuffer.wrap(this.buffer, start,
                            position - start + 1))
                    + "]--> end not defined yet";
        } else if (start > endExclusive) {
            if (start < position) {
                text = UTF_8.decode(java.nio.ByteBuffer.wrap(this.buffer, start,
                        position - start + 1)) + "][--> end not defined yet";
            } else {
                text = UTF_8.decode(java.nio.ByteBuffer.wrap(this.buffer, start,
                        1)) + "][--> end not defined yet";
            }
        } else {
            text = this.decodeToString();
        }
        return "StreamBufferChild [" + text + "]";
    }

    public String wholeBufferToString() {
        return UTF_8.decode(java.nio.ByteBuffer.wrap(this.buffer, 0, this.filledToExclusive)).toString();
    }

    public String remainingBufferToString() {
        return UTF_8.decode(java.nio.ByteBuffer.wrap(this.buffer, position, filledToExclusive - position)).toString();
    }
}
