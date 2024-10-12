package org.apache.jena.cimxml;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * High-performance parser for simplified CIM/RDF XML format
 * Optimized for UTF-8 encoded files with known patterns
 */
public class CIMXMLParser {

    // Parser state
    private enum State {
        LOOKING_FOR_TAG,
        IN_OPENING_TAG,
        IN_CLOSING_TAG,
        IN_ATTRIBUTE_NAME,
        IN_ATTRIBUTE_VALUE,
        IN_TEXT_CONTENT,
        IN_XML_DECLARATION,
        IN_COMMENT
    }

    // Precomputed byte patterns for fast matching
    private static final byte[] XML_DECL_START = "?xml".getBytes(StandardCharsets.UTF_8);
    private static final byte[] XML_DECL_END = "?>".getBytes(StandardCharsets.UTF_8);
    private static final byte[] RDF_RDF = "rdf:RDF".getBytes(StandardCharsets.UTF_8);
    private static final byte[] XMLNS = "xmlns".getBytes(StandardCharsets.UTF_8);
    private static final byte[] MD_FULLMODEL = "md:FullModel".getBytes(StandardCharsets.UTF_8);
    private static final byte[] COMMENT_START = "!--".getBytes(StandardCharsets.UTF_8);
    private static final byte[] COMMENT_END = "-->".getBytes(StandardCharsets.UTF_8);

    // Buffer and I/O
    private final FileChannel channel;
    private final ByteBuffer buffer;
    private boolean endOfFile = false;

    // Parser state
    private State currentState = State.LOOKING_FOR_TAG;
    private final StringBuilder textBuilder = new StringBuilder(1024);
    private final StringBuilder tagNameBuilder = new StringBuilder(64);
    private final StringBuilder attrNameBuilder = new StringBuilder(32);
    private final StringBuilder attrValueBuilder = new StringBuilder(256);

    // Attribute parsing state
    private boolean inQuotes = false;
    private byte quoteChar = 0;

    // Parsed data structures
    private final Map<String, String> namespaces = new HashMap<>();
    private final List<Element> elements = new ArrayList<>();
    private final Deque<Element> elementStack = new ArrayDeque<>();

    public CIMXMLParser(Path filePath, int bufferSize) throws IOException {
        final var fileSize = Files.size(filePath);
        this.channel = FileChannel.open(filePath, StandardOpenOption.READ);
        this.buffer = ByteBuffer.allocateDirect((fileSize < bufferSize) ? (int) fileSize : bufferSize);
        this.buffer.flip(); // Start with empty buffer
    }

    /**
     * Parse the entire XML file
     */
    public ParseResult parse() throws IOException {
        try {
            while (!endOfFile || buffer.hasRemaining()) {
                if (!buffer.hasRemaining()) {
                    if (!fillBuffer()) break;
                }

                byte currentByte = buffer.get();

                switch (currentState) {
                    case LOOKING_FOR_TAG -> handleLookingForTag(currentByte);
                    case IN_XML_DECLARATION -> handleXMLDeclaration(currentByte);
                    case IN_OPENING_TAG -> handleOpeningTag(currentByte);
                    case IN_CLOSING_TAG -> handleClosingTag(currentByte);
                    case IN_ATTRIBUTE_NAME -> handleAttributeName(currentByte);
                    case IN_ATTRIBUTE_VALUE -> handleAttributeValue(currentByte);
                    case IN_TEXT_CONTENT -> handleTextContent(currentByte);
                    case IN_COMMENT -> handleComment(currentByte);
                }
            }

            return new ParseResult(namespaces, elements);

        } finally {
            channel.close();
        }
    }

    private boolean fillBuffer() throws IOException {
        buffer.clear();
        int bytesRead = channel.read(buffer);
        buffer.flip();

        if (bytesRead < 1) {
            endOfFile = true;
            return false;
        }

        return true;
    }

    private void handleLookingForTag(byte b) throws IOException {
        if (b == '<') {
            // Check if this is XML declaration, comment, or regular tag
            if (matchesPatternAhead(XML_DECL_START)) {
                currentState = State.IN_XML_DECLARATION;
                consumePattern(XML_DECL_START);
            } else if (matchesPatternAhead(COMMENT_START)) {
                currentState = State.IN_COMMENT;
                consumePattern(COMMENT_START);
            } else if (peekNextByte() == '/') {
                currentState = State.IN_CLOSING_TAG;
                buffer.get(); // consume '/'
                tagNameBuilder.setLength(0);
            } else {
                currentState = State.IN_OPENING_TAG;
                tagNameBuilder.setLength(0);
            }
        } else if (!isWhitespace(b)) {
            // Text content
            currentState = State.IN_TEXT_CONTENT;
            textBuilder.setLength(0);
            textBuilder.append((char) b);
        }
    }

    private void handleXMLDeclaration(byte b) throws IOException {
        if (matchesPatternAhead(XML_DECL_END)) {
            consumePattern(XML_DECL_END);
            currentState = State.LOOKING_FOR_TAG;
            // XML declaration parsed - could extract version/encoding here
        }
        // For now, just consume until end
    }

    private void handleOpeningTag(byte b) throws IOException {
        if (b == '>') {
            // End of opening tag
            String tagName = tagNameBuilder.toString().trim();
            Element element = new Element(tagName);

            if (!elementStack.isEmpty()) {
                elementStack.peek().addChild(element);
            } else {
                elements.add(element);
            }

            // Check if self-closing tag
            if (tagName.endsWith("/")) {
                element.tagName = tagName.substring(0, tagName.length() - 1).trim();
                element.selfClosing = true;
            } else {
                elementStack.push(element);
            }

            currentState = State.LOOKING_FOR_TAG;
        } else if (isWhitespace(b) && tagNameBuilder.length() > 0) {
            // Space after tag name - expecting attributes
            currentState = State.IN_ATTRIBUTE_NAME;
            attrNameBuilder.setLength(0);
        } else if (b == '/') {
            // Self-closing tag
            tagNameBuilder.append('/');
        } else {
            tagNameBuilder.append((char) b);
        }
    }

    private void handleClosingTag(byte b) throws IOException {
        if (b == '>') {
            String tagName = tagNameBuilder.toString().trim();

            // Pop matching element from stack
            if (!elementStack.isEmpty() && elementStack.peek().tagName.equals(tagName)) {
                elementStack.pop();
            }

            currentState = State.LOOKING_FOR_TAG;
        } else {
            tagNameBuilder.append((char) b);
        }
    }

    private void handleAttributeName(byte b) throws IOException {
        if (b == '=') {
            currentState = State.IN_ATTRIBUTE_VALUE;
            attrValueBuilder.setLength(0);
        } else if (b == '>') {
            // End of tag without value
            finishCurrentElement();
        } else if (b == '/') {
            // Self-closing tag
            if (peekNextByte() == '>') {
                buffer.get(); // consume '>'
                finishCurrentElementAsSelfClosing();
            }
        } else if (!isWhitespace(b)) {
            attrNameBuilder.append((char) b);
        }
    }

    private void handleAttributeValue(byte b) throws IOException {

        if (!inQuotes && (b == '"' || b == '\'')) {
            inQuotes = true;
            quoteChar = b;
        } else if (inQuotes && b == quoteChar) {
            inQuotes = false;
            // Attribute complete
            String attrName = attrNameBuilder.toString().trim();
            String attrValue = attrValueBuilder.toString();

            // Handle namespaces
            if (attrName.startsWith("xmlns")) {
                String prefix = attrName.contains(":") ? attrName.substring(6) : "";
                namespaces.put(prefix, attrValue);
            }

            // Add attribute to current element
            if (!elementStack.isEmpty()) {
                elementStack.peek().addAttribute(attrName, attrValue);
            }

            currentState = State.IN_ATTRIBUTE_NAME;
            attrNameBuilder.setLength(0);
        } else if (inQuotes) {
            attrValueBuilder.append((char) b);
        } else if (b == '>') {
            finishCurrentElement();
        } else if (b == '/' && peekNextByte() == '>') {
            buffer.get(); // consume '>'
            finishCurrentElementAsSelfClosing();
        } else if (!inQuotes && !isWhitespace(b)) {
            // Unquoted attribute value
            attrValueBuilder.append((char) b);
        }
    }

    private void handleTextContent(byte b) throws IOException {
        if (b == '<') {
            // End of text content
            String text = textBuilder.toString().trim();
            if (!text.isEmpty() && !elementStack.isEmpty()) {
                elementStack.peek().textContent = text;
            }

            // Back up one byte to reprocess the '<'
            buffer.position(buffer.position() - 1);
            currentState = State.LOOKING_FOR_TAG;
        } else {
            textBuilder.append((char) b);
        }
    }

    private void handleComment(byte b) throws IOException {
        if (matchesPatternAhead(COMMENT_END)) {
            consumePattern(COMMENT_END);
            currentState = State.LOOKING_FOR_TAG;
        }
        // Just consume comment content
    }

    private void finishCurrentElement() {
        String tagName = tagNameBuilder.toString().trim();
        Element element = new Element(tagName);

        if (!elementStack.isEmpty()) {
            elementStack.peek().addChild(element);
        } else {
            elements.add(element);
        }

        elementStack.push(element);
        currentState = State.LOOKING_FOR_TAG;
    }

    private void finishCurrentElementAsSelfClosing() {
        String tagName = tagNameBuilder.toString().trim();
        Element element = new Element(tagName);
        element.selfClosing = true;

        if (!elementStack.isEmpty()) {
            elementStack.peek().addChild(element);
        } else {
            elements.add(element);
        }

        currentState = State.LOOKING_FOR_TAG;
    }

    // Utility methods
    private boolean matchesPatternAhead(byte[] pattern) throws IOException {
        if (buffer.remaining() < pattern.length) {
            return false;
        }

        int pos = buffer.position();
        for (int i = 0; i < pattern.length; i++) {
            if (buffer.get(pos + i) != pattern[i]) {
                return false;
            }
        }
        return true;
    }

    private void consumePattern(byte[] pattern) {
        buffer.position(buffer.position() + pattern.length - 1); // -1 because we already consumed first byte
    }

    private byte peekNextByte() throws IOException {
        if (!buffer.hasRemaining()) {
            if (!fillBuffer()) return 0;
        }
        return buffer.get(buffer.position());
    }

    private boolean isWhitespace(byte b) {
        return b == ' ' || b == '\t' || b == '\n' || b == '\r';
    }

    // Data structures
    public static class Element {
        public String tagName;
        public String textContent = "";
        public boolean selfClosing = false;
        public final Map<String, String> attributes = new HashMap<>();
        public final List<Element> children = new ArrayList<>();

        public Element(String tagName) {
            this.tagName = tagName;
        }

        public void addAttribute(String name, String value) {
            attributes.put(name, value);
        }

        public void addChild(Element child) {
            children.add(child);
        }

        @Override
        public String toString() {
            return String.format("Element{tagName='%s', attributes=%d, children=%d, text='%s'}",
                    tagName, attributes.size(), children.size(),
                    textContent.length() > 50 ? textContent.substring(0, 50) + "..." : textContent);
        }
    }

    public static class ParseResult {
        public final Map<String, String> namespaces;
        public final List<Element> rootElements;

        public ParseResult(Map<String, String> namespaces, List<Element> rootElements) {
            this.namespaces = new HashMap<>(namespaces);
            this.rootElements = new ArrayList<>(rootElements);
        }

        public void printSummary() {
            System.out.println("Namespaces found: " + namespaces.size());
            namespaces.forEach((prefix, uri) ->
                    System.out.println("  " + (prefix.isEmpty() ? "default" : prefix) + " -> " + uri));

            System.out.println("\nRoot elements: " + rootElements.size());
            rootElements.forEach(element -> printElement(element, 0));
        }

        private void printElement(Element element, int indent) {
            String indentStr = "  ".repeat(indent);
            System.out.println(indentStr + element);
            element.children.forEach(child -> printElement(child, indent + 1));
        }
    }
}