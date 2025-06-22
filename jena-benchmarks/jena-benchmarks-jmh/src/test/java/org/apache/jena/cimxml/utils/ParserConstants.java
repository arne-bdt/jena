package org.apache.jena.cimxml.utils;

public abstract class ParserConstants {
    public static final byte LEFT_ANGLE_BRACKET = (byte)'<';
    public static final byte RIGHT_ANGLE_BRACKET = (byte) '>';
    public static final byte WHITESPACE_SPACE = (byte)' ';
    public static final byte WHITESPACE_TAB = (byte)'\t';
    public static final byte WHITESPACE_NEWLINE = (byte)'\n';
    public static final byte WHITESPACE_CARRIAGE_RETURN = (byte)'\r';
    public static final byte QUESTION_MARK = (byte)'?';
    public static final byte EXCLAMATION_MARK = (byte)'!';
    public static final byte EQUALITY_SIGN = (byte)'=';
    public static final byte DOUBLE_QUOTE = (byte)'"';
    public static final byte SINGLE_QUOTE = (byte)'\'';
    public static final byte SLASH = (byte)'/';
    public static final byte DOUBLE_COLON = (byte)':';
    public static final byte SHARP = (byte)'#';
    public static final byte UNDERSCORE = (byte)'_';
    public static final byte SEMICOLON = (byte)';';
    public static final byte AMPERSAND = (byte)'&';
    public static final byte END_OF_STREAM = -1;

    public static final byte WHITESPACE_BLOOM_FILTER = WHITESPACE_SPACE | WHITESPACE_TAB | WHITESPACE_NEWLINE | WHITESPACE_CARRIAGE_RETURN;
    public static final byte END_OF_TAG_NAME_BLOOM_FILTER = WHITESPACE_BLOOM_FILTER | RIGHT_ANGLE_BRACKET | SLASH;
    public static final byte IGNORE_TAG_IF_FIRST_CHAR_IN_BOOM_FILTER = EXCLAMATION_MARK | QUESTION_MARK;
    public static final byte ANGLE_BRACKETS_BLOOM_FILTER = LEFT_ANGLE_BRACKET | RIGHT_ANGLE_BRACKET;

    public static boolean isAngleBrackets(byte b) {
        if(b == (ANGLE_BRACKETS_BLOOM_FILTER & b)) {
            switch (b) {
                case LEFT_ANGLE_BRACKET, RIGHT_ANGLE_BRACKET -> {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean isWhitespace(byte b) {
        return Character.isWhitespace(b);
//        if(b == (WHITESPACE_BLOOM_FILTER & b)) {
//            switch (b) {
//                case WHITESPACE_SPACE, WHITESPACE_TAB, WHITESPACE_NEWLINE, WHITESPACE_CARRIAGE_RETURN -> {
//                    return true;
//                }
//            }
//        }
//        return false;
    }

    public static boolean isEndOfTagName(byte b) {
        if(b == (END_OF_TAG_NAME_BLOOM_FILTER & b)) {
            switch (b) {
                case WHITESPACE_SPACE, RIGHT_ANGLE_BRACKET, SLASH, WHITESPACE_TAB, WHITESPACE_NEWLINE, WHITESPACE_CARRIAGE_RETURN -> {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean isTagToBeIgnoredDueToFirstChar(byte b) {
        if(b == (IGNORE_TAG_IF_FIRST_CHAR_IN_BOOM_FILTER & b)) {
            switch (b) {
                case EXCLAMATION_MARK, QUESTION_MARK-> {
                    return true;
                }
            }
        }
        return false;
    }
}
