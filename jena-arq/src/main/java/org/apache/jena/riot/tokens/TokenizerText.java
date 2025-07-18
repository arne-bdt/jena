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

package org.apache.jena.riot.tokens;

import static org.apache.jena.atlas.lib.Chars.*;
import static org.apache.jena.riot.system.RiotChars.*;

import java.util.NoSuchElementException;
import java.util.Objects;

import org.apache.jena.atlas.AtlasException;
import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.io.PeekReader;
import org.apache.jena.atlas.lib.Chars;
import org.apache.jena.riot.RiotParseException;
import org.apache.jena.riot.system.ErrorHandler;
import org.apache.jena.riot.system.RiotChars;
import org.apache.jena.sparql.ARQInternalErrorException;

/**
 * Tokenizer for the Turtle family of syntaxes.
 * Supports additional tokens.
 */
public final class TokenizerText implements Tokenizer
{
    // This class is performance critical.

    /* ==== Unicode and surrogates.
     *
     * == chars and ints
     *
     * Java's char is code unit of UTF-16. Strictly, "code points" are after decoding
     * from UTF-16 but for practical purposes a Java char is a code point, but a
     * codepoint can also be more than 16 bits, and a codepoint is not a surrogate.
     *
     * A surrogate pair is a UTF-16 way to encode codepoints beyond U+10FFFF. The
     * first (high part of the value) surrogate, is a 16-bit code value in the range
     * U+D800 to U+DBFF. The second (low) surrogate is a 16-bit code value in the
     * range U+DC00 to U+DFFF. Together these encode U+010000 to U+10FFFF
     *
     * codepoint = (high - 0xD800)*0x400 + (low - 0xDC00) + 0x010000
     *
     * UCS-2 is UTF-16 without surrogates, and so it is limited to U+0000 to U+FFFF.
     *
     * The code uses int when reading so that the non-codepoint 32bit -1 can be
     * returned to indicate end of file. Casting from int to char (java bytecode
     * "i2c") is silent truncation, no exception.
     *
     * Converting char to int is often implicit. It can cast and may be necessary to
     * all the right overloaded method/function. e.g. (int)ch for string formatti9ng
     * to %X (else char goes to Character object which is incompatible with %X.
     *
     * == RDF Strings
     *
     * RDF 1.2 introduces RDF String which is a sequence of Unicode Scalar Values
     * after decoding. Scalar values are code points U+0000 to U+10FFFF except for
     * surrogates. Surrogates, as pairs or alone, are illegal in RDF Strings.
     *
     * But Java strings are UTF-16 and use surrogate pairs. Jena needs to allow
     * well-formed surrogate pairs in Java strings.
     *
     * This included bad surrogate pairs written as \-u and \-U escape sequences or
     * the rare raw surrogate and Unicode escaped surrogate as a pair.
     *
     * Jena allows correctly used surrogate pairs because these can occur in the Java
     * ecosystem. Jena rejects lone surrogates, or an adjacent pair of surrogates
     * that are low-high (the wrong order).
     *
     * == Blank node labels
     *
     * Blank node labels exclude surrogates in the grammar (PN_CHARS_BASE) but allow
     * code points above U+FFFF so Jena accepts valid surrogate pairs.
     */

    // -- Configuration

    // One of these should be true.
    // Normally:
    //  CHECK_CODEPOINTS = true;
    //  CHECK_RDFSTRING = false;

    // Default=true. Whether to check for legal codepoint (i.e. only high-low surrogate pairs) while building strings.
    private static final boolean CHECK_CODEPOINTS = true;

    // Default=false. Whether to check for legal RDF strings (no ill formed use of surrogates) after building strings.
    private static final boolean CHECK_RDFSTRING = false;

    // Default=false. Allow some illegal characters in IRIs (probably causing rejection later when the IRI is parsed).
    private static final boolean VeryVeryLaxIRI = false;

    // Default=false. Spaces in IRI are illegal.
    private static final boolean AllowSpacesInIRI = false;

    // Controls related to raw use U+FFFD, the Unicode replacement character.
    // These are a sign that the input has been corrupted at some point, not
    // necessarily the file being read but in the way it was created.
    // They do occur in practice.

    // Replacement characters can occur in four places:
    // * IRIs -- illegal IRI syntax
    // * Strings -- in lexical forms.
    // * Prefixed names -- illegal syntax but they end the token so cause a different token.
    // * Blank node labels -- illegal syntax but they end the token so cause a different token.
    // Note that when the input is ASCII, U+FFFD occurs for non-ASCII characters.

    private final static boolean WarnOnReplacmentCharInIRI = false;
    private final static boolean WarnOnReplacmentCharInString = false;
    private final static boolean WarnOnReplacmentCharInPrefixedName = true;
    private final static boolean WarnOnReplacmentCharInBlankNodeLabel = true;

    // The code has the call points for checking tokens but it is generally better to
    // do the check later in the parsing process. In case a need arises, the code
    // remains, all compiled away by "if ( false )" (javac does not generate any
    // bytecodes and even if it it did, JIT will remove dead branches).
    private static final boolean CHECKER = false;
    // Optional checker.
    private final TokenChecker checker = null;

    // ----
    // Tokenizer state.

    // Character source
    private final PeekReader reader;
    // Whether whitespace between tokens includes newlines (in various forms).
    private final boolean singleLineMode;
    // The code assumes that errors throw exceptions and so stop parsing.
    private final ErrorHandler errorHandler;
    private Token token = null;
    private boolean finished = false;

    public static TokenizerTextBuilder create() { return new TokenizerTextBuilder(); }

    public static Tokenizer fromString(String string) { return create().fromString(string).build(); }

    /*package*/ static TokenizerText internal(PeekReader reader, boolean singleLineMode, ErrorHandler errorHandler) {
        return new TokenizerText(reader, singleLineMode, errorHandler);
    }

    private TokenizerText(PeekReader reader, boolean singleLineMode, ErrorHandler errorHandler) {
        this.reader = Objects.requireNonNull(reader, "PeekReader");
        this.singleLineMode = singleLineMode;
        this.errorHandler = Objects.requireNonNull(errorHandler, "ErrorHandler");
    }

    @Override
    public final boolean hasNext() {
        if ( finished )
            return false;
        if ( token != null )
            return true;

        try {
            skip();
            if ( reader.eof() ) {
                // close();
                finished = true;
                return false;
            }
            token = parseToken();
            if ( token == null ) {
                // close();
                finished = true;
                return false;
            }
            return true;
        } catch (AtlasException ex) {
            if ( ex.getCause() != null ) {
                if ( ex.getCause().getClass() == java.nio.charset.MalformedInputException.class )
                    throw new RiotParseException("Bad character encoding", reader.getLineNum(), reader.getColNum());
                throw new RiotParseException("Bad input stream [" + ex.getCause() + "]", reader.getLineNum(),
                                             reader.getColNum());
            }
            throw new RiotParseException("Bad input stream", reader.getLineNum(), reader.getColNum());
        }
    }

    @Override
    public final boolean eof() {
        return !hasNext();
    }

    @Override
    public final Token next() {
        if ( !hasNext() )
            throw new NoSuchElementException();
        Token t = token;
        token = null;
        return t;
    }

    @Override
    public final Token peek() {
        if ( !hasNext() )
            return null;
        return token;
    }

    @Override
    public void close() {
        IO.close(reader);
    }

    // ---- Machinery

    private void skip() {
        int ch = EOF;
        for (;;) {
            if ( reader.eof() )
                return;

            ch = reader.peekChar();
            if ( ch == CH_HASH ) {
                reader.readChar();
                // Comment. Skip to NL
                for (;;) {
                    ch = reader.peekChar();
                    if ( ch == EOF || isNewlineChar(ch) )
                        break;
                    reader.readChar();
                }
            }

            // Including excess newline chars from comment.
            if ( singleLineMode ) {
                if ( !isHorizontalWhitespace(ch) )
                    break;
            } else {
                if ( !isWhitespace(ch) )
                    break;
            }
            reader.readChar();
        }
    }

    private Token parseToken() {
        token = new Token(getLine(), getColumn());

        int ch = reader.peekChar();

        // ---- IRI, unless it's << or <<(
        // [spc] check is for LT.
        if ( ch == CH_LT ) {
            reader.readChar();
            // Look ahead on char
            int chPeek2 = reader.peekChar();
            if ( chPeek2 != CH_LT ) {
                // '<' not '<<'
                token.setImage(readIRI());
                token.setType(TokenType.IRI);
                if ( CHECKER )
                    checkURI(token.getImage());
                return token;
            }
            reader.readChar();
            // '<<' so far - maybe '<<('
            int chPeek3 = reader.peekChar();
            if ( chPeek3 != CH_LPAREN ) {
                // Not '<<(' - it's '<<'
                token.setType(TokenType.LT2);
                //token.setImage("<<");
                return token;
            }
            // It is '<<('
            reader.readChar();
            token.setType(TokenType.L_TRIPLE);
            //token.setImage("<<(");
            return token;
        }

        // ---- Literal
        if ( ch == CH_QUOTE1 || ch == CH_QUOTE2 ) {
            // The token type is STRING.
            // We incorporate this into a token for LITERAL_LANG or LITERAL_DT.
            token.setType(TokenType.STRING);

            reader.readChar();
            int ch2 = reader.peekChar();
            if ( ch2 == ch ) {
                reader.readChar(); // Read potential second quote.
                int ch3 = reader.peekChar();
                if ( ch3 == ch ) {
                    reader.readChar();     // Read potential third quote.
                    token.setImage(readStringQuote3(ch));
                    StringType st = (ch == CH_QUOTE1) ? StringType.LONG_STRING1 : StringType.LONG_STRING2;
                    token.setStringType(st);
                } else {
                    // Two quotes then a non-quote.
                    // Must be '' or ""
                    // No need to pushback characters as we know the lexical
                    // form is the empty string.
                    // if ( ch2 != EOF ) reader.pushbackChar(ch2);
                    // if ( ch1 != EOF ) reader.pushbackChar(ch1);
                    token.setImage("");
                    StringType st = (ch == CH_QUOTE1) ? StringType.STRING1 : StringType.STRING2;
                    token.setStringType(st);
                }
            } else {
                // One quote character.
                token.setImage(readStringQuote1(ch, ch));
                // Record exactly what form of STRING was seen.
                StringType st = (ch == CH_QUOTE1) ? StringType.STRING1 : StringType.STRING2;
                token.setStringType(st);
            }

            // White space after lexical part of a literal.
            skip();

            // Literal. Is it @ or ^^
            if ( reader.peekChar() == CH_AT ) {
                reader.readChar();
                // White space is not legal here.
                // The spec terminal is "LANGTAG" which includes the '@'.
                Token mainToken = new Token(token);
                mainToken.setType(TokenType.LITERAL_LANG);
                mainToken.setSubToken1(token);
                mainToken.setImage2(langTag());
                token = mainToken;
                if ( CHECKER )
                    checkLiteralLang(token.getImage(), token.getImage2());
            } else if ( reader.peekChar() == '^' ) {
                expect("^^");
                // White space is legal after a ^^.
                // It's not a good idea, but it is legal.
//                // Check no whitespace.
//                int nextCh = reader.peekChar();
//                if ( isWhitespace(nextCh) )
//                    exception("No whitespace after ^^ in literal with datatype");
                skip();

                // Stash current token.
                Token mainToken = new Token(token);
                mainToken.setSubToken1(token);
                mainToken.setImage(token.getImage());

                Token subToken = parseToken();
                if ( !subToken.isIRI() )
                    fatal("Datatype URI required after ^^ - URI or prefixed name expected");

                mainToken.setSubToken2(subToken);
                mainToken.setType(TokenType.LITERAL_DT);

                token = mainToken;
                if ( CHECKER )
                    checkLiteralDT(token.getImage(), subToken);
            } else {
                // Was a simple string.
                if ( CHECKER )
                    checkString(token.getImage());
            }
            return token;
        }

        if ( ch == CH_UNDERSCORE ) {
            reader.readChar();
            int ch2 = reader.peekChar();
            if ( ch2 == CH_COLON ) {
                reader.readChar();
                token.setImage(readBlankNodeLabel());
                token.setType(TokenType.BNODE);
                if ( CHECKER ) checkBlankNode(token.getImage());
                return token;
            }
            token.setType(TokenType.UNDERSCORE);
            /*token.setImage(CH_UNDERSCORE);*/
            return token;
        }

        // A directive (not part of a literal as lang tag)
        if ( ch == CH_AT ) {
            reader.readChar();
            token.setType(TokenType.DIRECTIVE);
            token.setImage(readWord(false));
            if ( CHECKER )
                checkDirective(token.getImage());
            return token;
        }

        // Variable
        if ( ch == CH_QMARK ) {
            reader.readChar();
            token.setType(TokenType.VAR);
            // Character set?
            token.setImage(readVarName());
            if ( CHECKER )
                checkVariable(token.getImage());
            return token;
        }

        //if ( ch == CH_DOLLAR ) {}

        switch(ch)
        {
            // DOT can start a decimal.
            case CH_DOT:
                reader.readChar();
                ch = reader.peekChar();
                if ( range(ch, '0', '9') ) {
                    // DOT DIGIT - it's a number.
                    // Reload the DOT.
                    reader.pushbackChar(CH_DOT);
                    boolean charactersConsumed = readNumber(CH_ZERO, false);
                    if ( charactersConsumed ) {
                        if ( CHECKER )
                            checkNumber(token.getImage(), token.getImage2());
                        return token;
                    }
                    // else it's DOT - drop through.
                }
                // It's DOT.
                token.setType(TokenType.DOT);
                return token;

            case CH_GT: {
                reader.readChar();
                int chPeek = reader.peekChar();
                if ( chPeek == CH_GT ) {
                    reader.readChar();
                    token.setType(TokenType.GT2);
                    return token;
                }
                token.setType(TokenType.GT);
                //token.setImage(">");
                return token;
            }

            case CH_SEMICOLON:  reader.readChar(); token.setType(TokenType.SEMICOLON); /*token.setImage(CH_SEMICOLON);*/ return token;
            case CH_COMMA:      reader.readChar(); token.setType(TokenType.COMMA);     /*token.setImage(CH_COMMA);*/ return token;

            // {| for RDF-star annotation syntax.
//            case CH_LBRACE:     reader.readChar(); token.setType(TokenType.LBRACE);    /*token.setImage(CH_LBRACE);*/ return token;
            case CH_LBRACE: {
                reader.readChar();
                int chPeek = reader.peekChar();
                if ( chPeek == CH_VBAR ) {
                    reader.readChar();
                    token.setType(TokenType.L_ANN);
                    return token;
                }
                token.setType(TokenType.LBRACE);
                return token;
            }
            case CH_RBRACE:     reader.readChar(); token.setType(TokenType.RBRACE);    /*token.setImage(CH_RBRACE);*/ return token;

            case CH_LPAREN:     reader.readChar(); token.setType(TokenType.LPAREN);    /*token.setImage(CH_LPAREN);*/ return token;

            // Can be ')' or ')>>'
            case CH_RPAREN: {
                // The ')'
                reader.readChar();
                int peek2 = reader.peekChar();
                if ( peek2 != '>') {
                    // Includes EOF.
                    token.setType(TokenType.RPAREN);
                    return token;
                }
                reader.readChar();
                int peek3 = reader.peekChar();
                if ( peek3 != '>') {
                    reader.pushbackChar(peek2);
                    token.setType(TokenType.RPAREN);
                    return token;
                }
                // It is ')>>'
                reader.readChar();
                token.setType(TokenType.R_TRIPLE);
                /*token.setImage(")>>");*/
                return token;
            }

            case CH_LBRACKET:   reader.readChar(); token.setType(TokenType.LBRACKET);  /*token.setImage(CH_LBRACKET);*/ return token;
            case CH_RBRACKET:   reader.readChar(); token.setType(TokenType.RBRACKET);  /*token.setImage(CH_RBRACKET);*/ return token;
            case CH_EQUALS:     reader.readChar(); token.setType(TokenType.EQUALS);    /*token.setImage(CH_EQUALS);*/ return token;
            case CH_SLASH:      reader.readChar(); token.setType(TokenType.SLASH);     /*token.setImage(CH_SLASH);*/ return token;
            case CH_RSLASH:     reader.readChar(); token.setType(TokenType.RSLASH);    /*token.setImage(CH_RSLASH);*/ return token;
//            case CH_VBAR:       reader.readChar(); token.setType(TokenType.VBAR);      /*token.setImage(CH_VBAR);*/ return token;

            // |} for RDF-star annotation syntax.
            case CH_VBAR: {
                reader.readChar();
                int chPeek = reader.peekChar();
                if ( chPeek == CH_RBRACE ) {
                    reader.readChar();
                    token.setType(TokenType.R_ANN);
                    return token;
                }
                token.setType(TokenType.VBAR);
                return token;
            }

            case CH_AMPHERSAND: reader.readChar(); token.setType(TokenType.AMPERSAND);/*token.setImage(CH_AMPHERSAND);*/ return token;
            // Specials (if prefix names processing is off)
            //case CH_COLON:      reader.readChar(); token.setType(TokenType.COLON); /*token.setImage(COLON);*/return token;
            // Done above with blank nodes.
            //case CH_UNDERSCORE: reader.readChar(); token.setType(TokenType.UNDERSCORE);/*token.setImage(CH_UNDERSCORE);*/ return token;
            case CH_LT:         reader.readChar(); token.setType(TokenType.LT);        /*token.setImage(CH_LT);*/ return token;
            case CH_STAR:       reader.readChar(); token.setType(TokenType.STAR);      /*token.setImage(CH_STAR);*/ return token;
            case CH_EMARK:      reader.readChar(); token.setType(TokenType.EMARK);     /*token.setImage(CH_EMARK);*/ return token;

            case CH_TILDE:      reader.readChar(); token.setType(TokenType.TILDE);     /*token.setImage(CH_TILDE);*/ return token;

            // VAR overrides
            //case CH_QMARK:      reader.readChar(); token.setType(TokenType.QMARK);   /*token.setImage(CH_EMARK);*/ return token;

            // Two character tokens && || GE >= , LE <=
            //TokenType.LE
            //TokenType.GE
            //TokenType.LOGICAL_AND
            //TokenType.LOGICAL_OR
        }

        // ---- Numbers.
        // A plain "+" and "-", not followed by an unsigned number are symbols.

        /*
        [16]    integer         ::=     ('-' | '+') ? [0-9]+
        [17]    double          ::=     ('-' | '+') ? ( [0-9]+ '.' [0-9]* exponent | '.' ([0-9])+ exponent | ([0-9])+ exponent )
                                        0.e0, .0e0, 0e0
        [18]    decimal         ::=     ('-' | '+')? ( [0-9]+ '.' [0-9]* | '.' ([0-9])+ | ([0-9])+ )
                                        0.0 .0 0.
        [19]    exponent        ::=     [eE] ('-' | '+')? [0-9]+
        []      hex             ::=     0x0123456789ABCDEFG

        */

        if ( ch == CH_PLUS || ch == CH_MINUS ) {
            reader.readChar();
            int ch2 = reader.peekChar();
            if ( !range(ch2, '0', '9') && ch2 != CH_DOT ) {
                // Not a number.
                if ( ch == CH_PLUS )
                    token.setType(TokenType.PLUS);
                else
                    token.setType(TokenType.MINUS);
                return token;
            }
            // ch2 not consumed.
            boolean charactersConsumed = readNumber(ch, false);
            if ( ! charactersConsumed ) {
                if ( ch == CH_PLUS )
                    token.setType(TokenType.PLUS);
                else
                    token.setType(TokenType.MINUS);
            }
            return token;
        }

        if ( range(ch, '0', '9')  ) {
            reader.readChar();
            if ( ch == '0' ) {
                // Is it "hex" -- 0x/0X ?
                boolean isHex = readPossibleHex();
                if ( isHex )
                    return token;
            }
            // Not hex.
            boolean charactersConsumed = readNumber(ch, true);
            if ( ! charactersConsumed ) {
                // Impossible.
                throw new InternalError("Seen digit but no number produced");
            }
            return token;
        }

        if ( isNewlineChar(ch) ) {
            //** - If collecting token image.
            //** resetStringBuilder();
            // Any number of NL and CR become one "NL" token.
            do {
                int ch2 = reader.readChar();
                // insertCodepointDirect(stringBuilder,ch2);
            } while (isNewlineChar(reader.peekChar()));
            token.setType(TokenType.NL);
            //** token.setImage(currentString());
            return token;
        }

        // Plain words and prefixes.
        //   Can't start with a number due to numeric test above.
        //   Can't start with a '_' due to blank node test above.
        // If we see a :, the first time it means a prefixed name else it's a token break.

        readPrefixedNameOrKeyword(token);

        if ( CHECKER ) checkKeyword(token.getImage());
        return token;
    }

    // ==== Manage the stringBuilder
    // Workspace for building token images.
    // Reusing a StringBuilder is faster than allocating a fresh one each time.
    // It should be possible to rename stringBuilder with no changes to the code anywhere outside these operations.s
    private final StringBuilder stringBuilder = new StringBuilder(200);

    private static final int NO_CODEPOINT = '\u0000';

   // -- Unicode sequences with the possibility of codepoints beyond U+FFFF
    /**
     * String with the possibility of a unicode surrogate or unicode escape.
     *
     * Pair with {@link #finishStringU(int)}
     */
    private void startStringU() {
        stringBuilder.setLength(0);
    }

    /**
     * Check terminates correctly and return string.
     * Pair with {@link #startStringU()}
     */
    private String finishStringU(int finalCodepoint) {
        if ( finalCodepoint != NO_CODEPOINT )
            fatal("Bad unpaired surrogate at end of string");
        return stringBuilder.toString();
    }

    // -- Strings without possible unicode surrogates

    /**
     * String with the possibility of a unicode surrogate or unicode escape.
     *
     * Pair with {@link #finishStringU(int)}
     */
    private void startStringNU() {
        stringBuilder.setLength(0);
    }

    /**
     * End processing a string.
     * Pair with {@link #startStringU()}
     */
    private String finishStringNU() {
        return stringBuilder.toString();
    }

    private int lengthStringBuilder()           { return stringBuilder.length(); }
    private void setStringBuilderLength(int x)  { stringBuilder.setLength(x); }

    private char charAt(int idx) { return stringBuilder.charAt(idx); }
    private void deleteCharAt(int idx) { stringBuilder.deleteCharAt(idx); }

    /** Insert codepoint. */
    private int insertCodepoint(int previousCP,int ch) {
        if ( Character.charCount(ch) == 1 ) {
            char ch16 = (char)ch;   // Safe, not truncating, because count = 1
            char rtn = 0;
            if ( CHECK_CODEPOINTS )
                rtn = checkCodepoint((char)previousCP, ch16);
            insertCodepointDirect(ch16);
            return rtn;
        } else {
            // Surrogate waiting?
            if ( CHECK_CODEPOINTS && (previousCP != NO_CODEPOINT) )
                fatal("Lone surrogate");
            if ( !Character.isDefined(ch) && !Character.isSupplementaryCodePoint(ch) )
                fatal("Illegal codepoint: 0x%04X", ch);
            // Only legal surrogate pairs at this point.
            char[] chars = Character.toChars(ch);
            stringBuilder.append(chars);
            return NO_CODEPOINT;
        }
    }

//    // Only high then low is allowed.
//    // Casting int to char is a 16 bit silent truncation (bytecode "i2c").
    private char checkCodepoint(char previousCP, char ch) {
        if ( ! Character.isSurrogate(ch) ) {
            if ( previousCP == NO_CODEPOINT )
                return NO_CODEPOINT;
            fatal("Bad surrogate (high surrogate not followed by a low surrogate): 0x%04X", (int)previousCP);
        }
        // Surrogate.
        if ( previousCP == NO_CODEPOINT ) {  // Effectively: is previousCodePoint a high surrogate?
            if ( Character.isHighSurrogate(ch) ) {
                // Park it
                return ch;
            }
            fatal("Bad surrogate (low surrogate not preceded by a high surrogate): 0x%04X", (int)ch);
        }
        // previousCodePoint != NO_CODEPOINT
        // Previous is a high surrogate

        if ( Character.isLowSurrogate(ch) ) {
            // high-low -- OK! Clear previous.
            return NO_CODEPOINT;
        }
        fatal("Bad surrogate (high surrogate not followed by low surrogate): 0x%04X", (int)previousCP);
        return NO_CODEPOINT;
    }

    // Insert codepoint, knowing that 'ch' is 16 bit and not a surrogate.
    private void insertCodepointDirect(int ch) {
        insertCodepointDirect((char)ch);
    }

    /** Insert codepoint, knowing that 'ch' is not a surrogate. */
    private void insertCodepointDirect(char ch) {
        stringBuilder.append(ch);
    }

    /** Snapshot (unchecked) string builder - for error messages. */
    private String currentString() { return stringBuilder.toString(); }

    // ====

    // [8]  IRIREF  ::= '<' ([^#x00-#x20<>"{}|^`\] | UCHAR)* '>'
    private String readIRI() {
        startStringU();
        int prevCP = NO_CODEPOINT;
        for (;;) {
            int ch = reader.readChar();
            switch(ch) {
                case EOF:
                    fatal("Broken IRI (End of file)"); return null;
                case NL:
                    fatal("Broken IRI (newline): %s", currentString()); return null;
                case CR:
                    fatal("Broken IRI (CR): %s", currentString()); return null;
                case CH_GT:
                    // Done!
                    String str = finishStringU(prevCP);
                    if ( CHECK_RDFSTRING )
                        checkRDFString(str);
                    return str;
                case CH_RSLASH:
                    ch = readUnicodeEscape();
                    // Don't check legality of ch (strict syntax at this point).
                    // IRI parsing will catch errors.
                    break;
                case CH_LT:
                    // Probably a corrupt file so treat as fatal.
                    fatal("Bad character in IRI (bad character: '<'): <%s[<]...>", currentString()); return null;
                case TAB:
                    error("Bad character in IRI (tab character): <%s[tab]...>", currentString()); break;
                case '{': case '}': case '"': case '|': case '^': case '`' :
                    if ( ! VeryVeryLaxIRI )
                        warning("Illegal character in IRI (codepoint U+%04X, '%c'): <%s[%c]...>", ch, (char)ch, currentString(), (char)ch);
                    break;
                case SPC:
                    if ( ! AllowSpacesInIRI )
                        error("Bad character in IRI (space): <%s[space]...>", currentString());
                    else
                        warning("Bad character in IRI (space): <%s[space]...>", currentString());
                    break;
                case REPLACEMENT:
                    if ( WarnOnReplacmentCharInIRI )
                        warning("Unicode replacement character U+FFFD in IRI");
                    break;
                default:
                    if ( ch <= 0x19 )
                        warning("Illegal character in IRI (control char 0x%02X): <%s[0x%02X]...>", ch, currentString(), ch);
            }
            prevCP = insertCodepoint(prevCP, ch);
        }
    }

    private void readPrefixedNameOrKeyword(Token token) {
        long posn = reader.getPosition();
        String prefixPart = readPrefixPart(); // Prefix part or keyword
        token.setImage(prefixPart);
        token.setType(TokenType.KEYWORD);
        int ch = reader.peekChar();
        if ( ch == CH_COLON ) {
            reader.readChar();
            token.setType(TokenType.PREFIXED_NAME);
            String ln = readLocalPart(); // Local part
            token.setImage2(ln);
            if ( CHECKER )
                checkPrefixedName(token.getImage(), token.getImage2());
        }

        // If we made no progress, nothing found, not even a keyword -- it's an
        // error.
        if ( posn == reader.getPosition() )
            fatal("Failed to find a prefix name or keyword: %c(%d;0x%04X)", ch, ch, ch);

        if ( CHECKER )
            checkKeyword(token.getImage());
    }

    /*
    The token rules from SPARQL and Turtle.
    PNAME_NS       ::=  PN_PREFIX? ':'
    PNAME_LN       ::=  PNAME_NS PN_LOCAL

    PN_CHARS_BASE  ::=  [A-Z] | [a-z] | [#x00C0-#x00D6] | [#x00D8-#x00F6] | [#x00F8-#x02FF] | [#x0370-#x037D] | [#x037F-#x1FFF]
                   |    [#x200C-#x200D] | [#x2070-#x218F] | [#x2C00-#x2FEF]
                   |    [#x3001-#xD7FF] | [#xF900-#xFDCF] | [#xFDF0-#xFFFD]
                   |    [#x10000-#xEFFFF]
    PN_CHARS_U  ::=  PN_CHARS_BASE | '_'
    PN_CHARS  ::=  PN_CHARS_U | '-' | [0-9] | #x00B7 | [#x0300-#x036F] | [#x203F-#x2040]

    PN_PREFIX  ::=  PN_CHARS_BASE ((PN_CHARS|'.')* PN_CHARS)?
    PN_LOCAL  ::=  (PN_CHARS_U | ':' | [0-9] | PLX ) ((PN_CHARS | '.' | ':' | PLX)* (PN_CHARS | ':' | PLX) )?
    PLX  ::=  PERCENT | PN_LOCAL_ESC
    PERCENT  ::=  '%' HEX HEX
    HEX  ::=  [0-9] | [A-F] | [a-f]
    PN_LOCAL_ESC  ::=  '\' ( '_' | '~' | '.' | '-' | '!' | '$' | '&' | "'" | '(' | ')' | '*' | '+' | ',' | ';' | '=' | '/' | '?' | '#' | '@' | '%' )
    */

    private String readPrefixPart() {
        // PN_PREFIX : also keywords.
        return readSegment(false);
    }

    private String readLocalPart() {
        // PN_LOCAL
        return readSegment(true);
    }

    // Read the prefix or localname part of a prefixed name.
    // Returns "" when there are no valid characters, e.g. prefix for ":foo" or local name for "ex:".
    private String readSegment(boolean isLocalPart) {
        // Prefix: PN_CHARS_BASE                       ((PN_CHARS|'.')* PN_CHARS)?
        // Local: ( PN_CHARS_U | ':' | [0-9] | PLX )   ((PN_CHARS | '.' | ':' | PLX)* (PN_CHARS | ':' | PLX) )?
        //    PN_CHARS_U is PN_CHARS_BASE and '_'

        // RiotChars has isPNChars_U_N for   ( PN_CHARS_U | [0-9] )

        int prevCP = NO_CODEPOINT;

        // -- Test first character
        int ch = reader.peekChar();
        if ( ch == EOF )
            return "";

        startStringU();

        if ( isLocalPart ) {
            if ( ch == CH_COLON ) {
                reader.readChar();
                prevCP = insertCodepoint(prevCP, ch);
            } else if ( ch == CH_PERCENT || ch == CH_RSLASH ) {
                // processPLX
                // read % or \
                reader.readChar();
                processPLX(ch);
                // prevCP = NO_CODEPOINT;
            } else if ( RiotChars.isPNChars_U_N(ch) ) {
                if ( WarnOnReplacmentCharInPrefixedName ) {
                    if ( ch == REPLACEMENT )
                        warning("Unicode replacement character U+FFFD in prefixed name");
                }
                prevCP = insertCodepoint(prevCP, ch);
                reader.readChar();
            } else {
                finishStringU(prevCP);
                return "";
            }
        } else {
            if ( !RiotChars.isPNCharsBase(ch) ) {
                finishStringU(prevCP);
                return "";
            }
            prevCP = insertCodepoint(prevCP, ch);
            reader.readChar();
        }
        // Done first character
        int chDot = 0;

        for (;;) {
            ch = reader.peekChar();
            boolean valid = false;
            if ( isLocalPart && (ch == CH_PERCENT || ch == CH_RSLASH) ) {
                reader.readChar();
                if ( chDot != 0 )
                    insertCodepointDirect(chDot);
                processPLX(ch);
                prevCP = NO_CODEPOINT;
                chDot = 0;
                continue;
            }

            // Single valid characters
            if ( isLocalPart && ch == CH_COLON )
                valid = true;
            else if ( isPNChars(ch) )
                valid = true;
            else if ( ch == CH_DOT )
                valid = true;
            else
                valid = false;

            if ( !valid )
                break; // Exit loop

            // Valid character.
            // Was there also a DOT in the previous loop?
            if ( chDot != 0 ) {
                insertCodepointDirect(chDot);
                chDot = 0;
            }

            if ( ch != CH_DOT ) {
                if ( WarnOnReplacmentCharInPrefixedName ) {
                    if ( ch == REPLACEMENT )
                        warning("Unicode replacement character U+FFFD in prefixed name");
                }
                prevCP = insertCodepoint(prevCP, ch);
            } else {
                // DOT - delay until next loop.
                chDot = ch;
            }
            reader.readChar();
        }

        // On exit, chDot may hold a character.

        if ( chDot == CH_DOT )
            // Unread it.
            reader.pushbackChar(chDot);
        return finishStringU(prevCP);
    }

    // Process PLX (percent or character escape for a prefixed name)
    private void processPLX(int ch) {
        if ( ch == CH_PERCENT ) {
            insertCodepointDirect(ch);
            ch = reader.peekChar();
            if ( !isHexChar(ch) )
                fatal("Not a hex character: '%c'", ch);
            insertCodepointDirect(ch);
            reader.readChar();

            ch = reader.peekChar();
            if ( !isHexChar(ch) )
                fatal("Not a hex character: '%c'", ch);
            insertCodepointDirect(ch);
            reader.readChar();
        } else if ( ch == CH_RSLASH ) {
            ch = readCharEscape();  // Does not allow Unicode escapes.
            insertCodepointDirect(ch);
        } else
            throw new ARQInternalErrorException("Not a '\\' or a '%' character");
    }

    /**
     * Apply any checks for "RDF String" to a string that has already had escape processing applied.
     * An RDF String is a sequence of codepoints in the range U+0000 to U+10FFFF, excluding surrogates.
     * Because this is java, we test for no non-paired surrogates.
     * A surrogate pair is high-low.
     * This check is performed in readIRI, readStrignQuote1, and readStringQuote3
     */
    private void checkRDFString(String string) {
        for ( int i = 0 ; i < string.length() ; i++ ) {
            // Not "codePointAt" which does surrogate processing.
            char ch = string.charAt(i);

            if ( ! Character.isValidCodePoint(ch) )
                warning("Illegal code point in \\U sequence value: 0x%08X", (int)ch);

            // Check surrogate pairs are pairs.
            if ( Character.isHighSurrogate(ch) ) {
                i++;
                if ( i == string.length() )
                    fatal("Bad surrogate pair (end of string):0x%04X", (int)ch);
                char ch1 = string.charAt(i);
                if ( ! Character.isLowSurrogate(ch1) ) {
                    fatal("Bad surrogate (high surrogate not followed by a low surrogate): 0x%04X", (int)ch1);
                }
            } else if ( Character.isLowSurrogate(ch) ) {
                fatal("Bad surrogate pair (low surrogate not preceded by a high surrogate): 0x%04X", (int)ch);
            }
        }
    }

    // Get characters between two markers.
    // String escapes are processed.
    private String readStringQuote1(int startCh, int endCh) {
        // Assumes the 1 character starting delimiter has been read.
        // Reads the terminating delimiter.
        startStringU();

        int prevCP = NO_CODEPOINT;
        for (;;) {
            int ch = reader.readChar();
            if ( WarnOnReplacmentCharInString ) {
                // Raw replacement char in a string.
                if ( ch == REPLACEMENT )
                    warning("Unicode replacement character U+FFFD in string");
            }
            if ( ch == NotACharacter || ch == ReverseOrderBOM )
                warning("Unicode non-character U+%04X in string", ch);
            if ( ch == EOF )
                fatal("Broken token: %s", currentString());
            else if ( ch == endCh ) {
                // Done!
                String str = finishStringU(prevCP);
                if ( CHECK_RDFSTRING )
                    checkRDFString(str);
                return str;
            } else if ( ch == NL )
                fatal("Broken token (newline in string)", currentString());
            else if ( ch == CR )
                fatal("Broken token (carriage return in string)", currentString());
            // Legal in Turtle/N-Triples - maybe warn?
//            else if ( ch == FF )
//                warning("Bad token (form feed in string)", currentString());
//            else if ( ch == VT )
//                fatal("Bad token (vertical tab in string)", currentString());
            else if ( ch == CH_RSLASH )
                // Allow escaped replacement character.
                ch = readLiteralEscape();
            prevCP = insertCodepoint(prevCP, ch);
        }
    }

    private String readStringQuote3(int quoteChar) {
        // Assumes the 3 character starting delimiter has been read.
        // Reads the terminating delimiter.
        startStringU();
        int prevCP = NO_CODEPOINT;
        for (;;) {
            int ch = reader.readChar();
            if ( WarnOnReplacmentCharInString ) {
                // Raw replacement char in a string.
                if ( ch == REPLACEMENT )
                    warning("Unicode replacement character U+FFFD in string");
            }
            if ( ch == EOF ) {
                fatal("Broken long string");
            } else if ( ch == quoteChar ) {
                if ( threeQuotes(quoteChar) ) {
                    String str = finishStringU(prevCP);
                    if ( CHECK_RDFSTRING )
                        checkRDFString(str);
                    return str;
                }
                // quote, not triple. It is a normal character.
            } else if ( ch == CH_RSLASH )
                ch = readLiteralEscape();
            prevCP = insertCodepoint(prevCP, ch);
        }
    }

    private String readWord(boolean leadingDigitAllowed) {
        return readWordSub(leadingDigitAllowed, false);
    }

    // A 'word' is used in several places:
    //   keyword
    //   prefix part of prefix name
    //   local part of prefix name (allows digits)

    static private char[] extraCharsWord = new char[] {'_', '.' , '-'};

    private String readWordSub(boolean leadingDigitAllowed, boolean leadingSignAllowed) {
        return readCharsWithExtras(leadingDigitAllowed, leadingSignAllowed, extraCharsWord, false);
    }

    // This array adds the other characters that can occurs in an internal variable name.
    // Variables can be created with SPARQL-illegal syntax to ensure they do not clash with
    // variables in the query from the application.
    // See ARQConstants.
    //   allocVarAnonMarker, allocVarMarker, globalVar, allocVarBNodeToVar, allocVarScopeHiding
    // but this set is wider and matches anywhere in the name after the first '?'.
    static private char[] extraCharsVar = new char[]{'_', '.', '-', '?', '@', '+', '/', '~'};

    private String readVarName() {
        return readCharsWithExtras(true, true, extraCharsVar, true);
    }

    private String readCharsWithExtras(boolean leadingDigitAllowed, boolean leadingSignAllowed, char[] extraChars, boolean allowFinalDot) {
        // No unicode escapes.
        startStringNU();
        int idx = 0;
        if ( !leadingDigitAllowed ) {
            int ch = reader.peekChar();
            if ( Character.isDigit(ch) )
                return "";
        }

        // Used for local part of prefix names =>
        if ( !leadingSignAllowed ) {
            int ch = reader.peekChar();
            if ( ch == '-' || ch == '+' )
                return "";
        }

        for (;; idx++) {
            int ch = reader.peekChar();

            if ( isAlphaNumeric(ch) || Chars.charInArray(ch, extraChars) ) {
                reader.readChar();
                insertCodepointDirect(ch);
                continue;
            } else
                // Inappropriate character.
                break;

        }

        if ( !allowFinalDot ) {
            // BAD : assumes pushbackChar is infinite.
            // Check is ends in "."
            while (idx > 0 && charAt(idx - 1) == CH_DOT) {
                // Push back the dot.
                reader.pushbackChar(CH_DOT);
                idx--;
                setStringBuilderLength(idx);
            }
        }
        return finishStringNU();
    }

    // BLANK_NODE_LABEL    ::=     '_:' (PN_CHARS_U | [0-9]) ((PN_CHARS | '.')* PN_CHARS)?

    private String readBlankNodeLabel() {
        startStringU();

        int prevCP = NO_CODEPOINT;
        // First character.
        {
            int ch = reader.peekChar();
            if ( ch == EOF )
                fatal("Blank node label missing (EOF found)");
            if ( isWhitespace(ch) )
                fatal("Blank node label missing");
            if ( !RiotChars.isPNChars_U_N(ch) )
                fatal("Blank node label does not start with alphabetic or _ : '%c'", (char)ch);
            reader.readChar();
            if ( WarnOnReplacmentCharInBlankNodeLabel ) {
                // Raw replacement char in a string.
                if ( ch == REPLACEMENT )
                    warning("Unicode replacement character U+FFFD in blank node label");
            }
            prevCP = insertCodepoint(prevCP, ch);
        }

        // Remainder. DOT can't be last so do a delay on that.

        int chDot = 0;

        for (;;) {
            int ch = reader.peekChar();
            if ( ch == EOF )
                break;

            // DOT magic.
            if ( !(RiotChars.isPNChars(ch) || ch == CH_DOT) )
                break;
            reader.readChar();

            if ( chDot != 0 ) {
                insertCodepointDirect(chDot);
                prevCP = NO_CODEPOINT;
                chDot = 0;
            }

            if ( ch != CH_DOT ) {
                if ( WarnOnReplacmentCharInBlankNodeLabel ) {
                    // Raw replacement char in a string.
                    if ( ch == REPLACEMENT )
                        warning("Unicode replacement character U+FFFD in blank node label");
                }
                prevCP = insertCodepoint(prevCP, ch);
            } else
                // DOT - delay until next loop.
                chDot = ch;
        }

        if ( chDot == CH_DOT )
            // Unread it.
            reader.pushbackChar(chDot);

        // if ( ! seen )
        // exception("Blank node label missing");
        return finishStringU(prevCP);
    }

    /*
     * Number, no sign.
     * [146]  INTEGER  ::=  [0-9]+
     * [147]  DECIMAL  ::=  [0-9]* '.' [0-9]+
     * [148]  DOUBLE  ::=  [0-9]+ '.' [0-9]* EXPONENT | '.' ([0-9])+ EXPONENT | ([0-9])+ EXPONENT
     */
    /**
     * Read a number.
     * <p>
     * On entry, {@code initialChar} is a seen and consumer character or {code CH_ZERO} (char 0x0000).
     * <p>
     * It parses {@code [0-9]* '.' [0-9]*}, then checks the outcome is not a single DOT, then adds an exponent.
     * If the number/significand is exactly '.', set the token to be DOT.
     * Note special code in sign processing for this.
     * <p>
     * HEX has already been handled.
     *
     * @returns true if the function consumed any characters.
     */
    private boolean readNumber(int initialChar, boolean isDigit) {
        // initial character is a +/- sign or 0.
        boolean isDouble = false;
        boolean hasDecimalPoint = false;
        boolean hasDigitsBeforeDot = false;
        boolean hasDigitsAfterDot = false;
        // DP = Decimal Point.
        int numDigitsBeforeDP = 0;
        int numDigitsAfterDP = 0;

        startStringNU();
        if ( initialChar != CH_ZERO ) { // char U+0000
            if ( initialChar == CH_PLUS || initialChar == CH_MINUS )
                insertCodepointDirect(initialChar);
            else if ( isDigit ) {
                insertCodepointDirect(initialChar);
                numDigitsBeforeDP = 1;
            }
        }

        int ch = reader.peekChar();
        numDigitsBeforeDP += readDigits();
        if ( numDigitsBeforeDP > 0 )
            hasDigitsBeforeDot = true;

        // DOT or integer.
        ch = reader.peekChar();
        if ( ch == CH_DOT ) {
            reader.readChar();
            insertCodepointDirect(CH_DOT);
            hasDecimalPoint = true;
            numDigitsAfterDP += readDigits();
            if ( numDigitsAfterDP > 0 )
                hasDigitsAfterDot = true;
        }

        if ( numDigitsBeforeDP == 0 && !hasDecimalPoint )
            // Possible a tokenizer error - should not have entered readNumber
            // in the first place.
            fatal("Unrecognized as number");

        if ( ! hasDigitsBeforeDot & ! hasDigitsAfterDot ) {
            // The number/significand/mantissa is exactly '.'
            // Don't do anything - there might be a preceding sign.
            if ( hasDecimalPoint )
                reader.pushbackChar(CH_DOT);
            return false;
        }

        if ( exponent() ) {
            isDouble = true;
        } else {
            // Final part - "decimal" 123. is an integer 123 and a DOT.
            if ( hasDecimalPoint && ! hasDigitsAfterDot ) {
                int N = lengthStringBuilder();
                // Reject the DOT which will be picked up next time.
                deleteCharAt(N-1);
                reader.pushbackChar(CH_DOT);
                hasDecimalPoint = false;
            }
        }

        token.setImage(finishStringNU());
        if ( isDouble )
            token.setType(TokenType.DOUBLE);
        else if ( hasDecimalPoint )
            token.setType(TokenType.DECIMAL);
        else
            token.setType(TokenType.INTEGER);
        return true;
    }

    // On entry, have seen and consumed a digit '0'
    private boolean readPossibleHex() {
        int ch2 = reader.peekChar();
        if ( ch2 != 'x' && ch2 != 'X' )
            return false;
        // It's HEX
        reader.readChar();
        startStringNU();
        insertCodepointDirect('0');
        insertCodepointDirect(ch2);
        // Error if no hex digits.
        readHex(reader);
        token.setImage(finishStringNU());
        token.setType(TokenType.HEX);
        return true;
    }

    private void readHex(PeekReader reader) {
        // Just after the 0x, which are in string builder.
        int x = 0;
        for (;;) {
            int ch = reader.peekChar();
            if ( !isHexChar(ch) )
                break;
            reader.readChar();
            insertCodepointDirect(ch);
            x++;
        }
        if ( x == 0 )
            fatal("No hex characters after %s", currentString());
    }

    private int readDigits() {
        int count = 0;
        for (;;) {
            int ch = reader.peekChar();
            if ( !range(ch, '0', '9') )
                break;
            reader.readChar();
            insertCodepointDirect(ch);
            count++;
        }
        return count;
    }

    private void readPossibleSign() {
        int ch = reader.peekChar();
        if ( ch == '-' || ch == '+' ) {
            reader.readChar();
            insertCodepointDirect(ch);
        }
    }

    // Assume we have read the first quote char.
    // On return:
    //   If false, have moved over no more characters (due to pushbacks)
    //   If true, at end of 3 quotes
    private boolean threeQuotes(int ch) {
        // reader.readChar(); // Read first quote.
        int ch2 = reader.peekChar();
        if ( ch2 != ch ) {
            // reader.pushbackChar(ch2);
            return false;
        }

        reader.readChar(); // Read second quote.
        int ch3 = reader.peekChar();
        if ( ch3 != ch ) {
            // reader.pushbackChar(ch3);
            reader.pushbackChar(ch2);
            return false;
        }

        // Three quotes.
        reader.readChar(); // Read third quote.
        return true;
    }

    private boolean exponent() {
        int ch = reader.peekChar();
        if ( ch != 'e' && ch != 'E' )
            return false;
        reader.readChar();
        insertCodepointDirect(ch);
        readPossibleSign();
        int x = readDigits();
        if ( x == 0 )
            fatal("Malformed double: %s", currentString());
        return true;
    }

    private String langTag() {
        startStringU();
        a2z();
        if ( lengthStringBuilder() == 0 )
            fatal("Bad language tag");

        boolean seenTextDirection = false;

        for (;;) {
            int ch = reader.peekChar();
            if ( ch == '-' ) {
                if ( seenTextDirection )
                   fatal("Bad language tag with base direction");
                reader.readChar();
                insertCodepointDirect(ch);
                int ch2 = reader.peekChar();
                if ( ch2 == '-' ) {
                    reader.readChar();
                    // base direction
                    insertCodepointDirect(ch2);
                    seenTextDirection = true;
                }
                int x = lengthStringBuilder();
                a2zN();
                if ( lengthStringBuilder() == x )
                    fatal("Bad language tag");
            } else
                break;
        }
        return finishStringU(NO_CODEPOINT).intern();
    }

    // ASCII-only e.g. in lang tags.
    private void a2z() {
        for (;;) {
            int ch = reader.peekChar();
            if ( isA2Z(ch) ) {
                reader.readChar();
                insertCodepointDirect(ch);
            } else
                return;
        }
    }

    private void a2zN() {
        for (;;) {
            int ch = reader.peekChar();
            if ( isA2ZN(ch) ) {
                reader.readChar();
                insertCodepointDirect(ch);
            } else
                return;
        }
    }

    @Override
    public long getColumn() {
        return reader.getColNum();
    }

    @Override
    public long getLine() {
        return reader.getLineNum();
    }

    // ---- Escape sequences

    // Read a unicode escape : does not allow \\ bypass
    private final int readUnicodeEscape() {
        int ch = reader.readChar();
        if ( ch == EOF )
            fatal("Broken escape sequence");

        switch (ch) {
            case 'u': return readUnicode4Escape();
            case 'U': return readUnicode8Escape();
            default:
                fatal("Illegal unicode escape sequence value: \\%c (0x%02X)", ch, ch);
        }
        return 0;
    }

    private final int readLiteralEscape() {
        int c = reader.readChar();
        if ( c == EOF )
            fatal("Escape sequence not completed");

        switch (c) {
            case 'n':   return NL;
            case 'r':   return CR;
            case 't':   return TAB;
            case 'f':   return '\f';
            case 'b':   return BSPACE;
            case '"':   return '"';
            case '\'':  return '\'';
            case '\\':  return '\\';
            case 'u':   return readUnicode4Escape();
            case 'U':   return readUnicode8Escape();
            default:
                fatal("Illegal escape sequence value: %c (0x%02X)",c , c);
                return 0;
        }
    }

    private final int readCharEscape() {
        // PN_LOCAL_ESC ::= '\' ( '_' | '~' | '.' | '-' | '!' | '$' | '&' | "'"
        //                | '(' | ')' | '*' | '+' | ',' | ';' | '=' | '/' | '?' | '#' | '@' | '%' )

        int c = reader.readChar();
        if ( c == EOF )
            fatal("Escape sequence not completed");

        switch (c) {
            case '_': case '~': case '.':  case '-':  case '!':  case '$':  case '&':
            case '\'':
            case '(':  case ')':  case '*':  case '+':  case ',':  case ';':
            case '=':  case '/':  case '?':  case '#':  case '@':  case '%':
                return c;
            default:
                fatal("illegal character escape value: \\%c", c);
                return 0;
        }
    }

    private final int readUnicode4Escape() {
        return readHexSequence(4);
    }

    private final int readUnicode8Escape() {
        int ch8 = readHexSequence(8);
        if ( ! Character.isValidCodePoint(ch8) )
            fatal("Illegal code point from \\U sequence value: 0x%08X", ch8);
        return ch8;
    }

    private final int readHexSequence(int N) {
        int x = 0;
        for (int i = 0; i < N; i++) {
            int d = readHexChar();
            if ( d < 0 )
                return -1;
            x = (x << 4) + d;
        }
        return x;
    }

    private final int readHexChar() {
        int ch = reader.readChar();
        if ( ch == EOF )
            fatal("Not a hexadecimal character (end of file)");

        int x = valHexChar(ch);
        if ( x != -1 )
            return x;
        fatal("Not a hexadecimal character: '%c'", (char)ch);
        return -1;
    }

    private boolean expect(String str) {
        for (int i = 0; i < str.length(); i++) {
            char want = str.charAt(i);
            if ( reader.eof() ) {
                fatal("End of input during expected string: %s", str);
                return false;
            }
            int inChar = reader.peekChar();
            if ( inChar != want ) {
                fatal("expected \"%s\"", str);
                return false;
            }
            reader.readChar();
        }
        return true;
    }

    /** Warning - can continue. */
    private void warning(String message, Object... args) {
        String msg = String.format(message, args);
        errorHandler.warning(msg, reader.getLineNum(), reader.getColNum());
    }

    /**
     * Error - at the tokenizer level, it can continue (with some junk) but it is a serious error and the
     * caller probably should treat as an error and stop.
     * @param message
     * @param args
     */
    private void error(String message, Object... args) {
        String msg = String.format(message, args);
        errorHandler.error(msg, reader.getLineNum(), reader.getColNum());
    }

    /** Structural error - unrecoverable - but reported as ERROR (FATAL can imply system fault) */
    private void fatal(String message, Object... args) {
        String msg = String.format(message, args);
        long line = reader.getLineNum();
        long col = reader.getColNum();
        errorHandler.fatal(msg, line, col);
        // We require that errors cause the tokenizer to stop so in case the
        // provided error handler does not, we throw an exception.
        throw new RiotParseException(message, line, col);
    }

    // ---- Routines to check tokens

    private void checkBlankNode(String blankNodeLabel) {
        if ( checker != null )
            checker.checkBlankNode(blankNodeLabel);
    }

    private void checkLiteralLang(String lexicalForm, String langTag) {
        if ( checker != null )
            checker.checkLiteralLang(lexicalForm, langTag);
    }

    private void checkLiteralDT(String lexicalForm, Token datatype) {
        if ( checker != null )
            checker.checkLiteralDT(lexicalForm, datatype);
    }

    private void checkString(String string) {
        if ( checker != null )
            checker.checkString(string);
    }

    private void checkURI(String uriStr) {
        if ( checker != null )
            checker.checkURI(uriStr);
    }

    private void checkNumber(String image, String datatype) {
        if ( checker != null )
            checker.checkNumber(image, datatype);
    }

    private void checkVariable(String tokenImage) {
        if ( checker != null )
            checker.checkVariable(tokenImage);
    }

    private void checkDirective(String directive) {
        if ( checker != null )
            checker.checkDirective(directive);
    }

    private void checkKeyword(String tokenImage) {
        if ( checker != null )
            checker.checkKeyword(tokenImage);
    }

    private void checkPrefixedName(String tokenImage, String tokenImage2) {
        if ( checker != null )
            checker.checkPrefixedName(tokenImage, tokenImage2);
    }

    private void checkControl(int code) {
        if ( checker != null )
            checker.checkControl(code);
    }
}
