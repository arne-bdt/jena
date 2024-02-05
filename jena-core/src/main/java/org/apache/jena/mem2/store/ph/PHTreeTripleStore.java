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
package org.apache.jena.mem2.store.ph;

import ch.ethz.globis.phtree.PhTree;
import org.apache.jena.atlas.lib.StreamOps;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.iterator.IteratorOfJenaSets;
import org.apache.jena.mem2.pattern.MatchPattern;
import org.apache.jena.mem2.pattern.PatternClassifier;
import org.apache.jena.mem2.store.TripleStore;
import org.apache.jena.mem2.store.fast.FastArrayBunch;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.SingletonIterator;

import java.util.function.Predicate;
import java.util.stream.Stream;


public class PHTreeTripleStore implements TripleStore {


    final PhTree<TripleBunch> bunches = PhTree.create(3);

    private int size;

    private static long[] getDimensions(Triple triple) {
        return new long[]{
                triple.getSubject().hashCode(),
                triple.getPredicate().hashCode(),
                triple.getObject().hashCode()
        };
    }

    @Override
    public void add(Triple triple) {
        final var key = getDimensions(triple);
        final var bunch = bunches.computeIfAbsent(key, k -> new TripleBunch());

        if(bunch.tryAdd(triple)) {
            size++;
        }
    }

    @Override
    public void remove(Triple triple) {
        final var key = getDimensions(triple);
        final var bunch = bunches.get(key);
        if(bunch != null && bunch.tryRemove(triple)) {
            size--;
            if(bunch.isEmpty()) {
                bunches.remove(key);
            }
        }
    }

    @Override
    public void clear() {
        bunches.clear();
        size = 0;
    }

    private PhTree.PhQuery<TripleBunch> query(MatchPattern  pattern, Triple tripleMatch) {
        final long[] min;
        final long[] max;
        switch (pattern) {
            case SUB_PRE_ANY -> {
                min = new long[]{tripleMatch.getSubject().hashCode(), tripleMatch.getPredicate().hashCode(), Integer.MIN_VALUE};
                max = new long[]{tripleMatch.getSubject().hashCode(), tripleMatch.getPredicate().hashCode(), Integer.MAX_VALUE};
            }
            case SUB_ANY_OBJ -> {
                min = new long[]{tripleMatch.getSubject().hashCode(), Integer.MIN_VALUE, tripleMatch.getObject().hashCode()};
                max = new long[]{tripleMatch.getSubject().hashCode(), Integer.MAX_VALUE, tripleMatch.getObject().hashCode()};
            }
            case SUB_ANY_ANY -> {
                min = new long[]{tripleMatch.getSubject().hashCode(), Integer.MIN_VALUE, Integer.MIN_VALUE};
                max = new long[]{tripleMatch.getSubject().hashCode(), Integer.MAX_VALUE, Integer.MAX_VALUE};
            }
            case ANY_PRE_OBJ -> {
                min = new long[]{Integer.MIN_VALUE, tripleMatch.getPredicate().hashCode(), tripleMatch.getObject().hashCode()};
                max = new long[]{Integer.MAX_VALUE, tripleMatch.getPredicate().hashCode(), tripleMatch.getObject().hashCode()};
            }
            case ANY_PRE_ANY -> {
                min = new long[]{Integer.MIN_VALUE, tripleMatch.getPredicate().hashCode(), Integer.MIN_VALUE};
                max = new long[]{Integer.MAX_VALUE, tripleMatch.getPredicate().hashCode(), Integer.MAX_VALUE};
            }
            case ANY_ANY_OBJ -> {
                min = new long[]{Integer.MIN_VALUE, Integer.MIN_VALUE, tripleMatch.getObject().hashCode()};
                max = new long[]{Integer.MAX_VALUE, Integer.MAX_VALUE, tripleMatch.getObject().hashCode()};
            }
            default -> throw new IllegalStateException("Unsupported pattern: " + pattern);
        }
        return this.bunches.query(min, max);
    }

    @Override
    public int countTriples() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    private boolean hasMatch(PhTree.PhQuery<TripleBunch> iter, Predicate<Triple> predicate) {
        while (iter.hasNext()) {
            if (iter.next().anyMatch(predicate)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean contains(Triple tripleMatch) {
        final var matchPattern = PatternClassifier.classify(tripleMatch);
        switch (matchPattern) {

            case SUB_ANY_ANY, ANY_PRE_ANY, ANY_ANY_OBJ, SUB_PRE_ANY, ANY_PRE_OBJ, SUB_ANY_OBJ:
                return hasMatch(query(matchPattern, tripleMatch), tripleMatch::matches);

            case SUB_PRE_OBJ:
                final var bunch = bunches.get(getDimensions(tripleMatch));
                if (bunch == null)
                    return false;
                return bunch.containsKey(tripleMatch);

            case ANY_ANY_ANY:
                return !this.isEmpty();

            default:
                throw new IllegalStateException("Unsupported pattern: " + matchPattern);
        }
    }

    @Override
    public Stream<Triple> stream() {
        return StreamOps.stream(bunches.queryExtent()).flatMap(TripleBunch::keyStream);
    }

    @Override
    public Stream<Triple> stream(Triple tripleMatch) {
        var pattern = PatternClassifier.classify(tripleMatch);
        switch (pattern) {

            case SUB_PRE_OBJ:
                final var bunch = bunches.get(getDimensions(tripleMatch));
                if (bunch == null)
                    return Stream.empty();
                return bunch.containsKey(tripleMatch) ? Stream.of(tripleMatch) : Stream.empty();

            case SUB_PRE_ANY, SUB_ANY_OBJ, SUB_ANY_ANY, ANY_PRE_OBJ, ANY_PRE_ANY, ANY_ANY_OBJ:
                return StreamOps.stream(query(pattern, tripleMatch))
                        .flatMap(TripleBunch::keyStream)
                        .filter(tripleMatch::matches);

            case ANY_ANY_ANY:
                return this.stream();

            default:
                throw new IllegalStateException("Unknown pattern classifier: " + PatternClassifier.classify(tripleMatch));
        }
    }

    @Override
    public ExtendedIterator<Triple> find(Triple tripleMatch) {
        var pattern = PatternClassifier.classify(tripleMatch);
        switch (pattern) {

            case SUB_PRE_OBJ:
                final var bunch = bunches.get(getDimensions(tripleMatch));
                if (bunch == null)
                    return NiceIterator.emptyIterator();
                return bunch.containsKey(tripleMatch) ? new SingletonIterator<>(tripleMatch) : NiceIterator.emptyIterator();

            case SUB_PRE_ANY, SUB_ANY_OBJ, SUB_ANY_ANY, ANY_PRE_OBJ, ANY_PRE_ANY, ANY_ANY_OBJ:
                return new IteratorOfJenaSets<>(query(pattern, tripleMatch))
                        .filterKeep(tripleMatch::matches);

            case ANY_ANY_ANY:
                return new IteratorOfJenaSets<>(bunches.queryExtent());

            default:
                throw new IllegalStateException("Unknown pattern classifier: " + PatternClassifier.classify(tripleMatch));
        }
    }

    private static class TripleBunch extends FastArrayBunch {
        @Override
        public boolean areEqual(Triple a, Triple b) {
            return a.equals(b);
        }
    }

}
