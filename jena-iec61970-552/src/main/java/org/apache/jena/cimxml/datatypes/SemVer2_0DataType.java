package org.apache.jena.cimxml.datatypes;

import org.apache.jena.datatypes.BaseDatatype;
import org.apache.jena.datatypes.DatatypeFormatException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SemVer2_0DataType extends BaseDatatype {

    // Choose the IRI you want to use for the datatype.
    public static final String URI = "https://semver.org/spec/v2.0.0.html";
    public static final SemVer2_0DataType INSTANCE = new SemVer2_0DataType();

    // Official SemVer 2.0.0 pattern (Java-escaped)
    private static final Pattern SEMVER = Pattern.compile(
            "^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)" +
                    "(?:-((?:0|[1-9]\\d*|[0-9A-Za-z-][0-9A-Za-z-]*)(?:\\.(?:0|[1-9]\\d*|[0-9A-Za-z-][0-9A-Za-z-]*))*))?" +
                    "(?:\\+([0-9A-Za-z-]+(?:\\.[0-9A-Za-z-]+)*))?$"
    );

    private SemVer2_0DataType() { super(URI); }

    @Override
    public Class<?> getJavaClass() { return SemVer2_0.class; }

    @Override
    public boolean isValid(String lexical) {
        return lexical != null && SEMVER.matcher(lexical).matches();
    }

    @Override
    public Object parse(String lexical) throws DatatypeFormatException {
        if (lexical == null) throw new DatatypeFormatException("null lexical form");
        Matcher m = SEMVER.matcher(lexical);
        if (!m.matches())
            throw new DatatypeFormatException(lexical, this, "Not a valid SemVer 2.0.0 string");

        try {
            int major = Integer.parseInt(m.group(1));
            int minor = Integer.parseInt(m.group(2));
            int patch = Integer.parseInt(m.group(3));
            List<String> pre = splitDot(m.group(4));
            List<String> build = splitDot(m.group(5));
            return new SemVer2_0(major, minor, patch, pre, build);
        } catch (Exception e) {
            throw new DatatypeFormatException(lexical, this, "Failed to parse SemVer: " + e.getMessage());
        }
    }

    @Override
    public String unparse(Object value) {
        if (value instanceof SemVer2_0 v) return v.canonical();
        if (value instanceof CharSequence s) return s.toString();
        throw new DatatypeFormatException("Not a SemVer value: " + value);
    }

    private static List<String> splitDot(String s) {
        if (s == null || s.isEmpty()) return Collections.emptyList();
        String[] parts = s.split("\\.");
        List<String> out = new ArrayList<>(parts.length);
        Collections.addAll(out, parts);
        return List.copyOf(out);
    }
}
