/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 *   SPDX-License-Identifier: Apache-2.0
 */

package org.apache.jena.mem.store.cow.strategies;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.IndexingStrategy;
import org.apache.jena.mem.store.cow.CowWriteTxn;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.jena.testing_framework.GraphHelper.node;
import static org.apache.jena.testing_framework.GraphHelper.triple;
import static org.junit.Assert.*;

/**
 * Cross-product test for the eager strategy's wildcard pattern lookups
 * under fork-and-mutate. For every partial pattern (6 cases) and every
 * lookup method (3 cases — {@code contains} / {@code stream} /
 * {@code find}), the test verifies that:
 * <ul>
 *   <li>after the writer mutates its fork, the source's lookups still
 *       return exactly the captured-at-fork answer (snapshot isolation
 *       holds for every pattern, not just the ones used in earlier
 *       tests);
 *   <li>the writer's lookups return the answer for its own
 *       post-mutation state.
 * </ul>
 * Each test seeds the store with triples chosen to populate
 * multi-element {@link org.apache.jena.mem.store.indexed.IndexList}s
 * shared across subjects/predicates/objects so the eager strategy's
 * intersection paths ({@code SUB_PRE_ANY}, {@code ANY_PRE_OBJ},
 * {@code SUB_ANY_OBJ}) actually exercise reverse-index arrays and
 * clone-on-touch.
 */
public class CowEagerWildcardForkTest {



    /** A probe triple combined with a human-readable label for failure messages. */
    private record Probe(String label, Triple match) {}

    /**
     * For the seeded triple set, build a list of probe patterns that
     * collectively exercise all 6 partial patterns. Each probe is chosen
     * to have a non-empty answer in the seed set so the lookup paths
     * actually return data.
     */
    private static List<Probe> probesForSeed() {
        return List.of(
                // SUB_ANY_ANY
                new Probe("SUB_ANY_ANY (subject=s1)",
                        Triple.createMatch(node("s1"), null, null)),
                new Probe("SUB_ANY_ANY (subject=s2)",
                        Triple.createMatch(node("s2"), null, null)),
                // ANY_PRE_ANY
                new Probe("ANY_PRE_ANY (predicate=p1)",
                        Triple.createMatch(null, node("p1"), null)),
                new Probe("ANY_PRE_ANY (predicate=p2)",
                        Triple.createMatch(null, node("p2"), null)),
                // ANY_ANY_OBJ
                new Probe("ANY_ANY_OBJ (object=o1)",
                        Triple.createMatch(null, null, node("o1"))),
                new Probe("ANY_ANY_OBJ (object=o2)",
                        Triple.createMatch(null, null, node("o2"))),
                // SUB_PRE_ANY
                new Probe("SUB_PRE_ANY (subject=s1, predicate=p1)",
                        Triple.createMatch(node("s1"), node("p1"), null)),
                new Probe("SUB_PRE_ANY (subject=s2, predicate=p2)",
                        Triple.createMatch(node("s2"), node("p2"), null)),
                // ANY_PRE_OBJ
                new Probe("ANY_PRE_OBJ (predicate=p1, object=o1)",
                        Triple.createMatch(null, node("p1"), node("o1"))),
                new Probe("ANY_PRE_OBJ (predicate=p2, object=o2)",
                        Triple.createMatch(null, node("p2"), node("o2"))),
                // SUB_ANY_OBJ
                new Probe("SUB_ANY_OBJ (subject=s1, object=o1)",
                        Triple.createMatch(node("s1"), null, node("o1"))),
                new Probe("SUB_ANY_OBJ (subject=s2, object=o2)",
                        Triple.createMatch(node("s2"), null, node("o2")))
        );
    }

    /** The expected answer for a probe is the subset of {@code truth} that matches it. */
    private static Set<Triple> expected(Set<Triple> truth, Triple probe) {
        return truth.stream()
                .filter(probe::matches)
                .collect(Collectors.toCollection(HashSet::new));
    }

    /**
     * For every probe in {@link #probesForSeed()}, assert that each of
     * {@code contains}/{@code stream}/{@code find} on {@code store}
     * agrees with {@code truth}.
     */
    private static void assertAllPatternsAgreeWith(CowWriteTxn store,
                                                   Set<Triple> truth, String tag) {
        for (Probe p : probesForSeed()) {
            Set<Triple> exp = expected(truth, p.match());
            // contains: true iff at least one match.
            assertEquals(tag + " contains: " + p.label(),
                    !exp.isEmpty(), store.contains(p.match()));
            // stream: returns exactly the matches.
            Set<Triple> viaStream = store.stream(p.match())
                    .collect(Collectors.toCollection(HashSet::new));
            assertEquals(tag + " stream: " + p.label(), exp, viaStream);
            // find: same set.
            Set<Triple> viaFind = store.find(p.match()).toSet();
            assertEquals(tag + " find: " + p.label(), exp, viaFind);
        }
    }

    /**
     * Build the canonical seed: 6 triples laid out so that each
     * subject, predicate, and object appears in at least 2 triples.
     * This guarantees every partial pattern exercises a multi-element
     * IndexList, and every two-component pattern exercises an actual
     * intersection (not a single-element shortcut).
     */
    private static Set<Triple> seedTriples() {
        return Set.of(
                triple("s1 p1 o1"),
                triple("s1 p1 o2"),
                triple("s1 p2 o1"),
                triple("s2 p1 o1"),
                triple("s2 p2 o2"),
                triple("s3 p2 o2"));
    }

    private static CowWriteTxn seededEager() {
        CowWriteTxn store =
                new CowWriteTxn(IndexingStrategy.EAGER);
        for (Triple x : seedTriples()) store.add(x);
        return store;
    }

    // ----- Sanity: seed itself satisfies all probes ------------------

    @Test
    public void seedSatisfiesAllProbes() {
        CowWriteTxn store = seededEager();
        assertAllPatternsAgreeWith(store, seedTriples(), "seed");
    }

    // ----- Fork adds a brand-new triple -----------------------------

    @Test
    public void forkAddBrandNewTriple_sourceUnchangedAcrossAllPatterns() {
        CowWriteTxn source = seededEager();
        Set<Triple> sourceTruth = seedTriples();

        CowWriteTxn fork = source.forkForWrite();
        Triple fresh = triple("s99 p99 o99");           // disjoint nodes
        fork.add(fresh);
        Set<Triple> forkTruth = new HashSet<>(sourceTruth);
        forkTruth.add(fresh);

        assertAllPatternsAgreeWith(source, sourceTruth, "source after fork-add(disjoint)");
        assertAllPatternsAgreeWith(fork, forkTruth, "fork after add(disjoint)");
    }

    @Test
    public void forkAddTripleSharingSubject_sourceSubjectListUnchanged() {
        CowWriteTxn source = seededEager();
        CowWriteTxn fork = source.forkForWrite();

        // Adding to s1 must clone s1's IndexList in the fork. The
        // source's view of s1 must still show exactly its 3 seed triples.
        fork.add(triple("s1 p3 o3"));

        Set<Triple> sourceTruth = seedTriples();
        Set<Triple> forkTruth = new HashSet<>(sourceTruth);
        forkTruth.add(triple("s1 p3 o3"));

        assertAllPatternsAgreeWith(source, sourceTruth, "source after fork-add(shared subject)");
        assertAllPatternsAgreeWith(fork, forkTruth, "fork after add(shared subject)");
    }

    @Test
    public void forkAddTripleSharingAllThreeComponents_eachIndexListClonedIndependently() {
        // The new triple shares its subject with one set of seeds, its
        // predicate with another set, its object with a third — exercises
        // three independent clone-on-touch operations on the fork.
        CowWriteTxn source = seededEager();
        CowWriteTxn fork = source.forkForWrite();

        fork.add(triple("s1 p2 o2"));        // shares s1, p2, o2 with seeds

        Set<Triple> sourceTruth = seedTriples();
        Set<Triple> forkTruth = new HashSet<>(sourceTruth);
        forkTruth.add(triple("s1 p2 o2"));

        assertAllPatternsAgreeWith(source, sourceTruth, "source after fork-add(all-shared)");
        assertAllPatternsAgreeWith(fork, forkTruth, "fork after add(all-shared)");
    }

    // ----- Fork removes a triple ------------------------------------

    @Test
    public void forkRemoveTriple_sourceListsUnchangedAcrossAllPatterns() {
        // Removing (s1,p1,o1) drops one entry from each of: subjectIndex.s1,
        // predicateIndex.p1, objectIndex.o1. The source's three lists for
        // those keys must still report all original triples.
        CowWriteTxn source = seededEager();
        CowWriteTxn fork = source.forkForWrite();

        fork.remove(triple("s1 p1 o1"));

        Set<Triple> sourceTruth = seedTriples();
        Set<Triple> forkTruth = new HashSet<>(sourceTruth);
        forkTruth.remove(triple("s1 p1 o1"));

        assertAllPatternsAgreeWith(source, sourceTruth, "source after fork-remove");
        assertAllPatternsAgreeWith(fork, forkTruth, "fork after remove");
    }

    @Test
    public void forkRemoveAllTriplesForOneSubject_sourceSubjectStillIndexed() {
        // Removing every triple with subject s1 in the fork should empty
        // the fork's subjectIndex.s1 entry but leave the source's untouched.
        CowWriteTxn source = seededEager();
        CowWriteTxn fork = source.forkForWrite();

        fork.remove(triple("s1 p1 o1"));
        fork.remove(triple("s1 p1 o2"));
        fork.remove(triple("s1 p2 o1"));

        Set<Triple> sourceTruth = seedTriples();
        Set<Triple> forkTruth = new HashSet<>(sourceTruth);
        forkTruth.remove(triple("s1 p1 o1"));
        forkTruth.remove(triple("s1 p1 o2"));
        forkTruth.remove(triple("s1 p2 o1"));

        assertAllPatternsAgreeWith(source, sourceTruth, "source after fork-removes-all-s1");
        assertAllPatternsAgreeWith(fork, forkTruth, "fork after removes-all-s1");
    }

    // ----- Mixed add + remove ---------------------------------------

    @Test
    public void forkInterleavedAddAndRemove_sourceAcrossAllPatterns() {
        CowWriteTxn source = seededEager();
        CowWriteTxn fork = source.forkForWrite();

        fork.remove(triple("s1 p1 o2"));
        fork.add(triple("s1 p1 o3"));
        fork.remove(triple("s2 p2 o2"));
        fork.add(triple("s2 p2 o3"));

        Set<Triple> sourceTruth = seedTriples();
        Set<Triple> forkTruth = new HashSet<>(sourceTruth);
        forkTruth.remove(triple("s1 p1 o2"));
        forkTruth.add(triple("s1 p1 o3"));
        forkTruth.remove(triple("s2 p2 o2"));
        forkTruth.add(triple("s2 p2 o3"));

        assertAllPatternsAgreeWith(source, sourceTruth, "source after fork-mixed");
        assertAllPatternsAgreeWith(fork, forkTruth, "fork after mixed");
    }

    // ----- Heavy churn through grow boundaries ----------------------

    @Test
    public void forkChurnThroughManyGrows_sourceRemainsConsistent() {
        // Drives the fork through many adds and removes, forcing
        // multiple grows + compactions inside both the spines and the
        // reverse-index arrays. The source's view across every wildcard
        // must remain exactly the original seed.
        CowWriteTxn source = seededEager();
        CowWriteTxn fork = source.forkForWrite();

        Random rnd = new Random(0xCAFE);
        Set<Triple> forkTruth = new HashSet<>(seedTriples());
        List<Triple> added = new ArrayList<>(Arrays.asList(
                triple("s1 p1 o3"), triple("s1 p3 o1"), triple("s4 p1 o4"),
                triple("s4 p2 o4"), triple("s5 p3 o5"), triple("s2 p3 o5"),
                triple("s6 p1 o6"), triple("s6 p4 o6"), triple("s7 p2 o7"),
                triple("s8 p4 o8"), triple("s9 p5 o9"), triple("s3 p5 o5")));
        for (Triple x : added) {
            fork.add(x);
            forkTruth.add(x);
        }
        // Now remove half at random.
        for (int i = 0; i < added.size(); i += 2) {
            fork.remove(added.get(i));
            forkTruth.remove(added.get(i));
        }
        // And add some more, trying to share components with seeds.
        Triple[] more = {
                triple("s1 p4 o4"), triple("s2 p4 o4"),
                triple("s3 p1 o1"), triple("s4 p2 o2")};
        for (Triple x : more) {
            fork.add(x);
            forkTruth.add(x);
        }

        // Source: every probe still reports the original truth.
        assertAllPatternsAgreeWith(source, seedTriples(),
                "source after fork churn");
        // Fork: every probe reports the post-mutation truth.
        assertAllPatternsAgreeWith(fork, forkTruth,
                "fork after churn");

        // Sanity: source is genuinely independent — it's still the seed.
        assertEquals(seedTriples().size(), source.countTriples());
    }

    // ----- Two-step: fork → fork (commit-equivalent chain) ----------

    @Test
    public void chainedForks_eachLinkPreservesPredecessorAcrossAllPatterns() {
        // Simulate a publish chain: source -> fork1 (writer commits
        // logically to become "the new published") -> fork2.
        // Each predecessor must keep its own consistent view across all
        // patterns regardless of what its successors do.
        CowWriteTxn source = seededEager();
        CowWriteTxn fork1 = source.forkForWrite();
        fork1.add(triple("s1 p3 o3"));
        fork1.remove(triple("s2 p1 o1"));

        CowWriteTxn fork2 = fork1.forkForWrite();
        fork2.add(triple("s4 p4 o4"));
        fork2.remove(triple("s1 p1 o1"));

        Set<Triple> sourceTruth = seedTriples();

        Set<Triple> fork1Truth = new HashSet<>(sourceTruth);
        fork1Truth.add(triple("s1 p3 o3"));
        fork1Truth.remove(triple("s2 p1 o1"));

        Set<Triple> fork2Truth = new HashSet<>(fork1Truth);
        fork2Truth.add(triple("s4 p4 o4"));
        fork2Truth.remove(triple("s1 p1 o1"));

        assertAllPatternsAgreeWith(source, sourceTruth, "source after chain");
        assertAllPatternsAgreeWith(fork1, fork1Truth, "fork1 after chain");
        assertAllPatternsAgreeWith(fork2, fork2Truth, "fork2 after chain");
    }

    // ----- Empty-result probes after fork removes the only triple --

    @Test
    public void forkRemovesOnlyMatchingTriple_emptyResultsForAffectedProbes() {
        // (s3,p2,o2) is the only seed with subject s3. Removing it in
        // the fork must make SUB_ANY_ANY(s3) return empty in the fork
        // but still 1 in the source.
        CowWriteTxn source = seededEager();
        CowWriteTxn fork = source.forkForWrite();
        fork.remove(triple("s3 p2 o2"));

        Triple s3probe = Triple.createMatch(node("s3"), null, null);
        assertTrue("source still has s3-rooted triples",
                source.contains(s3probe));
        assertEquals(1, source.find(s3probe).toSet().size());
        assertEquals(1, source.stream(s3probe).count());

        assertFalse("fork has none after remove", fork.contains(s3probe));
        assertEquals(0, fork.find(s3probe).toSet().size());
        assertEquals(0, fork.stream(s3probe).count());
    }
}
