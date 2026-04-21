package org.apache.jena.mem2.store.fast;

import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.NoSuchElementException;
import java.util.function.Consumer;

public class DoubleIndexListsIterator extends NiceIterator<Triple> {

    private final FastHashedBunchMap subjects;
    private final int[] subjectIndices;
    private final int[] tripleIndices;
    private final Runnable checkForConcurrentModification;
    private int pos;

    public DoubleIndexListsIterator(final FastHashedBunchMap subjects, final DoubleIndexList indexList, final Runnable checkForConcurrentModification) {
        pos = indexList.size();
        this.subjects = subjects;
        this.subjectIndices = indexList.getSubjectIndices();
        this.tripleIndices = indexList.getElementIndices();
        this.checkForConcurrentModification = checkForConcurrentModification;
    }

    @Override
    public boolean hasNext() {
        return 0 < pos;
    }

    @Override
    public Triple next() {
        checkForConcurrentModification.run();
        if(0 < pos) {
            return subjects
                    .getValueAt(subjectIndices[--pos])
                    .getKeyAt(tripleIndices[pos]);
        }
        throw new NoSuchElementException();
    }

    @Override
    public void forEachRemaining(Consumer<? super Triple> action) {
        while (0 < pos) {
            action.accept(subjects
                    .getValueAt(subjectIndices[--pos])
                    .getKeyAt(tripleIndices[pos]));
        }
        checkForConcurrentModification.run();
    }
}
