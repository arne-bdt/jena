package org.apache.jena.cimxml.utils;

import java.io.InputStream;

public class StreamBufferRoot {

    /**
     * Input stream from which the data is read.
     * This stream is expected to be used by the child buffers to read data.
     */
    InputStream inputStream;
    /**
     * The current buffer that is being filled with data.
     * This is used to handover remaining bytes from one child to the next.
     */
    StreamBufferChild lastUsedChildBuffer;

    public StreamBufferRoot() {

    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public void setInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    public StreamBufferChild getLastUsedChildBuffer() {
        return lastUsedChildBuffer;
    }

    public void setLastUsedChildBuffer(StreamBufferChild lastUsedChildBuffer) {
        this.lastUsedChildBuffer = lastUsedChildBuffer;
    }
}
