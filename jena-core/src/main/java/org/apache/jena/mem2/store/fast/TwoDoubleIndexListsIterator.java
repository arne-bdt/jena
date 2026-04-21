package org.apache.jena.mem2.store.fast;

import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.NoSuchElementException;
import java.util.function.Consumer;

public class TwoDoubleIndexListsIterator extends NiceIterator<Triple> {

    private final FastHashedBunchMap subjects;
    private final int[] smallerSubjectIndices;
    private final int[] smallerTripleIndices;
    private final int[] largerSubjectIndices;
    private final int[] largerTripleIndices;
    private final int indexLarger;
    private final int largerSize;
    private final Runnable checkForConcurrentModification;
    private int pos;
    boolean hasNext = false;

    public TwoDoubleIndexListsIterator(final FastHashedBunchMap subjects,
                                       final DoubleIndexList indexListA, int indexA,
                                       final DoubleIndexList indexListB, int indexB,
                                       final Runnable checkForConcurrentModification) {
        this.subjects = subjects;
        if(indexListA.size() < indexListB.size()) {
            pos = indexListA.size();
            smallerSubjectIndices = indexListA.getSubjectIndices();
            smallerTripleIndices = indexListA.getElementIndices();
            largerSubjectIndices = indexListB.getSubjectIndices();
            largerTripleIndices = indexListB.getElementIndices();
            indexLarger = indexB;
            largerSize = indexListB.size();
        } else {
            pos = indexListB.size();
            smallerSubjectIndices = indexListB.getSubjectIndices();
            smallerTripleIndices = indexListB.getElementIndices();
            largerSubjectIndices = indexListA.getSubjectIndices();
            largerTripleIndices = indexListA.getElementIndices();
            indexLarger = indexA;
            largerSize = indexListA.size();
        }
        this.checkForConcurrentModification = checkForConcurrentModification;
    }

    @Override
    public boolean hasNext() {
        if(hasNext)
            return true;
        while (-1 < --pos) {
            final var triples = subjects.getValueAt(smallerSubjectIndices[pos]);
            final var lIndex = triples.getIndex(smallerTripleIndices[pos], indexLarger);

            if(lIndex < largerSize
                && largerSubjectIndices[lIndex] == smallerSubjectIndices[pos]
                && largerTripleIndices[lIndex] == smallerTripleIndices[pos]) {
                return hasNext = true;
            }
        }
        return false;
    }

    @Override
    public Triple next() {
        checkForConcurrentModification.run();
        if(hasNext || hasNext()) {
            hasNext = false;
            return subjects
                    .getValueAt(smallerSubjectIndices[pos])
                    .getKeyAt(smallerTripleIndices[pos]);
        }
        throw new NoSuchElementException();
    }

    @Override
    public void forEachRemaining(Consumer<? super Triple> action) {
        while (-1 < --pos) {
            final var triples = subjects.getValueAt(smallerSubjectIndices[pos]);
            final var lIndex = triples.getIndex(smallerTripleIndices[pos], indexLarger);

            if(lIndex < largerSize
                    && largerSubjectIndices[lIndex] == smallerSubjectIndices[pos]
                    && largerTripleIndices[lIndex] == smallerTripleIndices[pos]) {
                action.accept(subjects
                        .getValueAt(smallerSubjectIndices[pos])
                        .getKeyAt(smallerTripleIndices[pos]));
            }
        }
        checkForConcurrentModification.run();
    }
}
