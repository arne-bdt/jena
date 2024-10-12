/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.cimxml;

import org.apache.commons.io.input.BufferedFileChannelInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;

public class CIMParser {

    private static final int MAX_BUFFER_SIZE = 64 * 4096; // 256 KB
    private static final Charset charset = StandardCharsets.UTF_8;
    private static final byte LEFT_ANGLE_BRACKET = (byte)'<';
    private static final byte RIGHT_ANGLE_BRACKET = (byte) '>';
    private static final byte WHITESPACE_SPACE = (byte)' ';
    private static final byte WHITESPACE_TAB = (byte)'\t';
    private static final byte WHITESPACE_NEWLINE = (byte)'\n';
    private static final byte WHITESPACE_CARRIAGE_RETURN = (byte)'\r';
    private static final byte END_OF_STREAM = -1;
    private static final byte[] XML_DECL_START = "<?xml".getBytes(StandardCharsets.UTF_8);
    private static final byte[] XML_DECL_END = "?>".getBytes(StandardCharsets.UTF_8);
    private static final byte[] RDF_RDF = "<rdf:RDF".getBytes(StandardCharsets.UTF_8);
    private static final byte[] XMLNS = "xmlns".getBytes(StandardCharsets.UTF_8);
    private static final byte[] COMMENT_START = "<!--".getBytes(StandardCharsets.UTF_8);
    private static final byte[] COMMENT_END = "-->".getBytes(StandardCharsets.UTF_8);
    private static final int MAX_MARK_SIZE = 1024;
    private static final int TAG_OR_ATTRIBUTE_NAME_MAX_LENGTH = 1024; // Maximum length for tag or attribute names

    private final Path filePath;
    private final FileChannel fileChannel;
    private final InputStream inputStream;



    // Parser state
    private enum State {
        LOOKING_FOR_XML_DECLARATION,
        LOOKING_FOR_TAG,
        LOOKING_FOR_TAG_NAME,
        LOOKING_FOR_ATTIBUTE_NAME,
        IN_OPENING_TAG,
        AT_END_OF_OPENING_TAG,
        IN_CLOSING_TAG,
        IN_ATTRIBUTE_NAME,
        IN_ATTRIBUTE_VALUE,
        IN_TEXT_CONTENT,
        IN_XML_DECLARATION,
        IN_COMMENT,
        END
    }

    public CIMParser(final Path filePath) {
        if(filePath == null) {
            throw new IllegalArgumentException("File path cannot be null");
        }
        this.filePath = filePath;
        this.fileChannel = null;
        this.inputStream = null;
    }

    public CIMParser(final FileChannel fileChannel) {
        if(fileChannel == null) {
            throw new IllegalArgumentException("File channel cannot be null");
        }
        this.filePath = null;
        this.fileChannel = fileChannel;
        this.inputStream = null;
    }

    public CIMParser(final InputStream inputStream) {
        if(inputStream == null) {
            throw new IllegalArgumentException("InputStream cannot be null");
        }
        this.filePath = null;
        this.fileChannel = null;
        this.inputStream = inputStream;
    }

    private class ParserException extends Exception {
        public ParserException(String message) {
            super(message);
        }
        public ParserException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public void parse() throws IOException, ParserException {
        if(inputStream != null)
            this.parse(inputStream);
        else if (fileChannel != null)
            this.parse(fileChannel);
        else if (filePath != null)
            this.parse(filePath);
        else
            throw new IllegalStateException("No input source provided for parsing");

    }

    private void parse(Path filePath) throws IOException, ParserException {
        try (var channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            parse(channel);
        }
    }

    private void parse(FileChannel channel) throws IOException, ParserException {
        final long fileSize = channel.size();
        try(final var is = new BufferedFileChannelInputStream.Builder()
                .setFileChannel(channel)
                .setOpenOptions(StandardOpenOption.READ)
                .setBufferSize((fileSize < MAX_BUFFER_SIZE) ? (int) fileSize : MAX_BUFFER_SIZE)
                .get()) {
            parse(is);
        }
    }

    private void parse(InputStream inputStream) throws IOException, ParserException {
        if(!inputStream.markSupported()) {
            throw new ParserException("InputStream must support mark/reset operations");
        }
        var state = State.LOOKING_FOR_TAG;
        while (state != State.END) {
            state = switch (state) {
                case LOOKING_FOR_XML_DECLARATION -> State.LOOKING_FOR_TAG;
                case LOOKING_FOR_TAG -> handleLookingForTag(inputStream);
                case LOOKING_FOR_TAG_NAME -> handleLookingForTagName(inputStream);
                default -> { throw new IllegalStateException("Unexpected state: " + state); }
            };
        }
    }

    private static final byte WHITESPACE_BLOOM_FILTER = WHITESPACE_SPACE | WHITESPACE_TAB | WHITESPACE_NEWLINE | WHITESPACE_CARRIAGE_RETURN;
    private static final byte END_OF_TAG_NAME_BLOOM_FILTER = WHITESPACE_BLOOM_FILTER | RIGHT_ANGLE_BRACKET;

    private static boolean isWhitespace(byte b) {
        if(b == (WHITESPACE_BLOOM_FILTER & b)) {
            switch (b) {
                case WHITESPACE_SPACE, WHITESPACE_TAB, WHITESPACE_NEWLINE, WHITESPACE_CARRIAGE_RETURN -> {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isEndOfTagName(byte b) {
        if(b == (END_OF_TAG_NAME_BLOOM_FILTER & b)) {
            switch (b) {
                case WHITESPACE_SPACE, RIGHT_ANGLE_BRACKET, WHITESPACE_TAB, WHITESPACE_NEWLINE, WHITESPACE_CARRIAGE_RETURN -> {
                    return true;
                }
            }
        }
        return false;
    }

    private static class FixedByteArrayBuffer {
        private final byte[] buffer;
        private int position;

        public FixedByteArrayBuffer(int size) {
            this.buffer = new byte[size];
            this.position = 0;
        }

        public void reset() {
            position = 0;
        }

        public void reset(byte firstByte) {
            buffer[0] = firstByte;
            position = 1;
        }

        public void append(byte b) {
            if (position < buffer.length) {
                buffer[position++] = b;
            } else {
                throw new IllegalStateException("Buffer overflow");
            }
        }

        public int length() {
            return position;
        }

        public boolean isFull() {
            return position == buffer.length;
        }

        public byte[] toByteArray() {
            return Arrays.copyOf(buffer, position);
        }
    }

    private final FixedByteArrayBuffer fixedBuffer = new FixedByteArrayBuffer(TAG_OR_ATTRIBUTE_NAME_MAX_LENGTH);

    public State handleLookingForTagName(InputStream inputStream) throws IOException, ParserException {
        byte b = (byte) inputStream.read();
        if(isEndOfTagName(b)) {
            // If the first byte is not a valid start of tag name, we throw an exception
            throw new ParserException("Unexpected character at the start of tag name: " + (char) b);
        }
        fixedBuffer.reset(b);
        while ((b = (byte) inputStream.read()) != -1) {
            if(isEndOfTagName(b)) {
                if(b == RIGHT_ANGLE_BRACKET) {
                    return State.AT_END_OF_OPENING_TAG;
                }
                return State.LOOKING_FOR_ATTIBUTE_NAME;
            }
            fixedBuffer.append(b);
            if(fixedBuffer.isFull()) {
                throw new ParserException("Tag name exceeds maximum length of " + fixedBuffer.length() + " characters");
            }
        }
        throw new ParserException("Unexpected end of stream while looking for tag name");
    }

    public State handleLookingForTag(InputStream inputStream) throws IOException, ParserException {
        byte b;
        while ((b = (byte) inputStream.read()) != -1) {
            if (LEFT_ANGLE_BRACKET == b) {
                return State.LOOKING_FOR_TAG_NAME;
            }
        }
        return State.END;
    }
}
