package org.apache.jena.cimxml.datatypes;

import java.math.BigInteger;
import java.util.List;

public final class SemVer2_0 implements Comparable<SemVer2_0> {
    public final int major, minor, patch;
    public final List<String> preRelease; // identifiers; empty if none
    public final List<String> build;      // identifiers; ignored for precedence

    public SemVer2_0(int major, int minor, int patch, List<String> preRelease, List<String> build) {
        this.major = major;
        this.minor = minor;
        this.patch = patch;
        this.preRelease = preRelease == null ? List.of() : List.copyOf(preRelease);
        this.build = build == null ? List.of() : List.copyOf(build);
    }

    public boolean isPreRelease() { return !preRelease.isEmpty(); }

    public String canonical() {
        StringBuilder sb = new StringBuilder();
        sb.append(major).append('.').append(minor).append('.').append(patch);
        if (!preRelease.isEmpty()) sb.append('-').append(String.join(".", preRelease));
        if (!build.isEmpty()) sb.append('+').append(String.join(".", build));
        return sb.toString();
    }

    @Override public String toString() { return canonical(); }

    @Override
    public int compareTo(SemVer2_0 o) {
        int c;
        if ((c = Integer.compare(major, o.major)) != 0) return c;
        if ((c = Integer.compare(minor, o.minor)) != 0) return c;
        if ((c = Integer.compare(patch, o.patch)) != 0) return c;
        // Precedence: absence of pre-release > presence
        if (preRelease.isEmpty() && o.preRelease.isEmpty()) return 0;
        if (preRelease.isEmpty()) return 1;
        if (o.preRelease.isEmpty()) return -1;
        // Compare pre-release identifiers
        int len = Math.min(preRelease.size(), o.preRelease.size());
        for (int i = 0; i < len; i++) {
            String a = preRelease.get(i);
            String b = o.preRelease.get(i);
            boolean aNum = isNumeric(a), bNum = isNumeric(b);
            if (aNum && bNum) {
                int r = new BigInteger(a).compareTo(new BigInteger(b));
                if (r != 0) return r;
            } else if (aNum != bNum) {
                return aNum ? -1 : 1; // numeric < non-numeric
            } else {
                int r = a.compareTo(b); // ASCII sort, case-sensitive
                if (r != 0) return r;
            }
        }
        return Integer.compare(preRelease.size(), o.preRelease.size());
    }

    private static boolean isNumeric(String s) {
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (ch < '0' || ch > '9') return false;
        }
        return !s.isEmpty();
    }

    @Override public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof SemVer2_0 other)) return false;
        return major == other.major && minor == other.minor && patch == other.patch
                && preRelease.equals(other.preRelease) && build.equals(other.build);
    }
    @Override public int hashCode() {
        int r = Integer.hashCode(major);
        r = 31*r + Integer.hashCode(minor);
        r = 31*r + Integer.hashCode(patch);
        r = 31*r + preRelease.hashCode();
        r = 31*r + build.hashCode();
        return r;
    }
}
