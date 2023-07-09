package org.apache.jena.sparql.core.mem2;

import org.apache.jena.graph.Capabilities;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.mem2.GraphMem2Fast;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

public class FastDeltaGraph extends GraphBase {

    private final Graph base;
    private final Graph additions = new GraphMem2Fast();
    private final Set<Triple> deletions = new HashSet<>();

    public FastDeltaGraph(Graph base) {
        super();
        if (base != null)
            throw new IllegalArgumentException("base graph must be null");
        if (base.getCapabilities().handlesLiteralTyping())
            throw new IllegalArgumentException("base graph must not handle literal typing");
        if (!base.getCapabilities().sizeAccurate())
            throw new IllegalArgumentException("base graph must be size accurate");
        this.base = base;
    }

    public Iterator<Triple> getAdditions() {
        return additions.find();
    }

    public Iterator<Triple> getDeletions() {
        return deletions.iterator();
    }

    public boolean hasChanges() {
        return !additions.isEmpty() || !deletions.isEmpty();
    }

    public Graph getBase() {
        return base;
    }

    @Override
    public void performAdd(Triple t) {
        if (!base.contains(t))
            additions.add(t);
        deletions.remove(t);
    }

    @Override
    public void performDelete(Triple t) {
        additions.delete(t);
        if (base.contains(t))
            deletions.add(t);
    }

    @Override
    public boolean dependsOn(Graph other) {
        return base.equals(other) || base.dependsOn(other);
    }

    @Override
    protected boolean graphBaseContains(Triple t) {
        if (t.isConcrete()) {
            if (deletions.contains(t)) {
                return false;
            } else {
                return additions.contains(t) || base.contains(t);
            }
        } else {
            return graphBaseFind(t).hasNext();
        }
    }

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple triplePattern) {
        return additions.find(triplePattern)
                .andThen(base.find(triplePattern)
                        .filterDrop(deletions::contains));
    }

    @Override
    public ExtendedIterator<Triple> find() {
        return additions.find()
                .andThen(base.find()
                        .filterDrop(deletions::contains));
    }

    @Override
    public Stream<Triple> stream() {
        return Stream.concat(
                additions.stream(),
                base.stream().filter(t -> !deletions.contains(t)));
    }

    @Override
    public Stream<Triple> stream(Node s, Node p, Node o) {
        return Stream.concat(
                additions.stream(s, p, o),
                base.stream(s, p, o).filter(t -> !deletions.contains(t)));
    }

    @Override
    public void close() {
        super.close();
        base.close();
        additions.close();
        deletions.clear();
    }

    @Override
    public int graphBaseSize() {
        return base.size() + additions.size() - deletions.size();
    }

    @Override
    public Capabilities getCapabilities() {
        if (null == capabilities) {
            capabilities = new Capabilities() {
                @Override
                public boolean sizeAccurate() {
                    return true;
                }

                @Override
                public boolean addAllowed() {
                    return true;
                }

                @Override
                public boolean deleteAllowed() {
                    return true;
                }

                @Override
                public boolean handlesLiteralTyping() {
                    return false;
                }
            };
        }
        return capabilities;
    }
}
