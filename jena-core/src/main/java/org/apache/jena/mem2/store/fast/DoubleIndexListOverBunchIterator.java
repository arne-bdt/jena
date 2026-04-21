package org.apache.jena.mem2.store.fast;

import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.NoSuchElementException;
import java.util.function.Consumer;

public class DoubleIndexListOverBunchIterator extends NiceIterator<Triple> {

    private final FastTripleBunch bunch;
    private final int subjectIndex;
    private final DoubleIndexList indexList;
    private final Runnable checkForConcurrentModification;
    private int pos;
    boolean hasNext = false;

    public DoubleIndexListOverBunchIterator(final DoubleIndexList indexList,
                                            final int index,
                                            final FastTripleBunch bunch,
                                            final int subjectIndex,
                                            final Runnable checkForConcurrentModification) {
        this.indexList = indexList;
        this.pos = indexList.size();
        this.bunch = bunch;
        this.subjectIndex = subjectIndex;
        this.checkForConcurrentModification = checkForConcurrentModification;
    }

    @Override
    public boolean hasNext() {
        if(hasNext)
            return true;
        while (-1 < --pos) {
            if(indexList.getSubjectIndexAt(pos) == subjectIndex) {
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
            return bunch.getKeyAt(indexList.getElementIndexAt(pos));
        }
        throw new NoSuchElementException();
    }

    @Override
    public void forEachRemaining(Consumer<? super Triple> action) {
        while (-1 < --pos) {
            if(indexList.getSubjectIndexAt(pos) == subjectIndex) {
                action.accept(bunch.getKeyAt(indexList.getElementIndexAt(pos)));
            }
        }
        checkForConcurrentModification.run();
    }
}
