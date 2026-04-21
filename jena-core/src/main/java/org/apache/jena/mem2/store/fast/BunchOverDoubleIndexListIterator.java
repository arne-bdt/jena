package org.apache.jena.mem2.store.fast;

import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.NoSuchElementException;
import java.util.function.Consumer;

public class BunchOverDoubleIndexListIterator extends NiceIterator<Triple> {

    private final FastTripleBunch bunch;
    private final int subjectIndex;
    private final DoubleIndexList indexList;
    private final int listSize;
    private final int index;
    private final Runnable checkForConcurrentModification;
    private int pos;
    boolean hasNext = false;

    public BunchOverDoubleIndexListIterator(final FastTripleBunch bunch,
                                            final int subjectIndex,
                                            final DoubleIndexList indexList,
                                            final int index,
                                            final Runnable checkForConcurrentModification) {
        this.bunch = bunch;
        this.subjectIndex = subjectIndex;
        this.indexList = indexList;
        this.listSize = indexList.size();
        this.index = index;
        this.pos = bunch.size();
        this.checkForConcurrentModification = checkForConcurrentModification;
    }

    @Override
    public boolean hasNext() {
        if(hasNext)
            return true;
        while (-1 < --pos) {
            final var li = bunch.getIndex(pos, index);
            if (li < listSize
                && indexList.getSubjectIndexAt(li) == subjectIndex
                && indexList.getElementIndexAt(li) == pos) {
                hasNext = true;
            }
        }
        return false;
    }

    @Override
    public Triple next() {
        checkForConcurrentModification.run();
        if(hasNext || hasNext()) {
            hasNext = false;
            return bunch.getKeyAt(pos);
        }
        throw new NoSuchElementException();
    }

    @Override
    public void forEachRemaining(Consumer<? super Triple> action) {
        while (-1 < --pos) {
            final var li = bunch.getIndex(pos, index);
            if (li < listSize
                    && indexList.getSubjectIndexAt(li) == subjectIndex
                    && indexList.getElementIndexAt(li) == pos) {
                action.accept(bunch.getKeyAt(pos));
            }
        }
        checkForConcurrentModification.run();
    }
}
