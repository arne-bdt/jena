package org.apache.jena.cimxml.utils;

import java.io.IOException;
import java.util.function.Consumer;

import static org.apache.jena.cimxml.utils.ParserConstants.*;

public class DecodingTextByteBuffer extends StreamBufferChild {
    protected int lastAmpersandPosition = -1; // Position of the last '&' character, used for decoding

    public DecodingTextByteBuffer(StreamBufferRoot parent, int size) {
        super(parent, size);
    }

    @Override
    public void reset() {
        super.reset();
        this.lastAmpersandPosition = -1;
    }

    @Override
    protected void afterConsumeCurrent() {
        switch (buffer[position]) {
            case AMPERSAND -> lastAmpersandPosition = position; // Store the position of the last '&'
            case SEMICOLON -> {
                var charsBetweenAmpersandAndSemicolon = position - lastAmpersandPosition - 1;
                switch (charsBetweenAmpersandAndSemicolon) {
                    case 2: {
                        if (buffer[lastAmpersandPosition + 2] == 't') {
                            if (buffer[lastAmpersandPosition + 1] == 'l') {
                                buffer[lastAmpersandPosition] = LEFT_ANGLE_BRACKET; // &lt;

                                // move remaining data to the left
                                System.arraycopy(buffer, position + 1,
                                        buffer, lastAmpersandPosition + 1,
                                        filledToExclusive - position);

                                filledToExclusive -= 3; // Reduce filledToExclusive by 3 for &lt;
                                position = lastAmpersandPosition;
                                lastAmpersandPosition = -1; // Reset last ampersand position
                                return;
                            } else if (buffer[lastAmpersandPosition + 1] == 'g') {
                                buffer[lastAmpersandPosition] = RIGHT_ANGLE_BRACKET; // &gt;

                                // move remaining data to the left
                                System.arraycopy(buffer, position + 1,
                                        buffer, lastAmpersandPosition + 1,
                                        filledToExclusive - position);

                                filledToExclusive -= 3; // Reduce filledToExclusive by 3 for &gt;
                                position = lastAmpersandPosition;
                                lastAmpersandPosition = -1; // Reset last ampersand position
                                return;
                            }
                        }
                        break;
                    }
                    case 3: {
                        if (buffer[lastAmpersandPosition + 3] == 'p'
                                && buffer[lastAmpersandPosition + 2] == 'm'
                                && buffer[lastAmpersandPosition + 1] == 'a') {
                            buffer[lastAmpersandPosition] = AMPERSAND; // &amp;

                            // move remaining data to the left
                            System.arraycopy(buffer, position + 1,
                                    buffer, lastAmpersandPosition + 1,
                                    filledToExclusive - position);

                            filledToExclusive -= 4; // Reduce filledToExclusive by 4 for &amp;
                            position = lastAmpersandPosition;
                            lastAmpersandPosition = -1; // Reset last ampersand position
                            return;
                        }
                        break;
                    }
                    case 4: {
                        if (buffer[lastAmpersandPosition + 3] == 'o') {
                            if (buffer[lastAmpersandPosition + 1] == 'q'
                                    && buffer[lastAmpersandPosition + 2] == 'u'
                                    && buffer[lastAmpersandPosition + 4] == 't') {
                                buffer[lastAmpersandPosition] = DOUBLE_QUOTE; // &quot;

                                // move remaining data to the left
                                System.arraycopy(buffer, position + 1,
                                        buffer, lastAmpersandPosition + 1,
                                        filledToExclusive - position);

                                filledToExclusive -= 5; // Reduce filledToExclusive by 5 for &quot;
                                position = lastAmpersandPosition;
                                lastAmpersandPosition = -1; // Reset last ampersand position
                                return;
                            } else if (buffer[lastAmpersandPosition + 2] == 'p'
                                    && buffer[lastAmpersandPosition + 4] == 's'
                                    && buffer[lastAmpersandPosition + 1] == 'a') {
                                buffer[lastAmpersandPosition] = SINGLE_QUOTE; // &apos;

                                // move remaining data to the left
                                System.arraycopy(buffer, position + 1,
                                        buffer, lastAmpersandPosition + 1,
                                        filledToExclusive - position);

                                filledToExclusive -= 5; // Reduce filledToExclusive by 5 for &apos;
                                position = lastAmpersandPosition;
                                lastAmpersandPosition = -1; // Reset last ampersand position
                                return;
                            }
                        }
                        break;
                    }
                }
            }
        }
    }
}
