package org.apache.jena.mem.sorted.experiment;

import org.apache.jena.graph.Triple;

import java.util.PriorityQueue;

public class TripleWithHashCodes {

    public final Triple triple;
    public final int hashCodeForSubject;
    public final int hashCodeForPredicate;
    public final int hashCodeForObject;
    public final int hashCode;

    private TripleWithHashCodes(final Triple triple,
                                final int hashCodeForSubject,
                                final int hashCodeForPredicate,
                                final int hashCodeForObject) {
        this.triple = triple;
        this.hashCode = triple.hashCode();
        this.hashCodeForSubject = hashCodeForSubject;
        this.hashCodeForPredicate = hashCodeForPredicate;
        this.hashCodeForObject = hashCodeForObject;
    }

    public int getCombindedHashCodeForSubjectAndPredicate() {
        return (hashCodeForSubject >> 1) ^ hashCodeForPredicate;
    }

    public int getCombindedHashCodeForSubjectAndObject() {
        return (hashCodeForSubject >> 1) ^ hashCodeForObject;
    }

    public static TripleWithHashCodes bySubject(final Triple triple, final int hashCodeForSubject) {
        return new TripleWithHashCodes(triple,
                hashCodeForSubject,
                triple.getPredicate().getIndexingValue().hashCode(),
                triple.getObject().getIndexingValue().hashCode());
    }

    public static TripleWithHashCodes byPredicate(final Triple triple, final int hashCodeForPredicate) {
        return new TripleWithHashCodes(triple,
                triple.getSubject().getIndexingValue().hashCode(),
                hashCodeForPredicate,
                triple.getObject().getIndexingValue().hashCode());
    }

    public static TripleWithHashCodes byObject(final Triple triple, final int hashCodeForObject) {
        return new TripleWithHashCodes(triple,
                triple.getSubject().getIndexingValue().hashCode(),
                triple.getPredicate().getIndexingValue().hashCode(),
                hashCodeForObject);
    }

    @Override
    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;

        TripleWithHashCodes that = (TripleWithHashCodes) o;

        if (hashCodeForSubject != that.hashCodeForSubject) return false;
        if (hashCodeForObject != that.hashCodeForObject) return false;
        if (hashCodeForPredicate != that.hashCodeForPredicate) return false;
        return triple.equals(that.triple);
    }

    @Override
    public int hashCode() {
        return triple.hashCode();
    }
}
