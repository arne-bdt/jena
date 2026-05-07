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

package org.apache.jena.mem.store.cow;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Property-style fuzz test for the COW snapshot guarantee at the
 * {@link CowIndexedSetTripleStore} layer (i.e. <i>below</i> the graph
 * lifecycle). Each "snapshot" we hold here is the result of a
 * {@link CowIndexedSetTripleStore#forkForWrite()} where the original is
 * <b>retained</b> (representing the published reference) and the writer is
 * mutated freely. The published store must continue to report the exact
 * triple set it held at the moment of the fork, regardless of the writer's
 * subsequent activity.
 * <p>
 * The test runs many random commit-equivalent rounds, taking new
 * "snapshots" along the way and verifying their views never drift.
 */
public class CowIndexedSetTripleStoreFuzzTest {

    private static final int NUM_ROUNDS = 2_000;
    private static final int OPS_PER_ROUND = 30;
    private static final int NUM_SNAPSHOTS_TO_HOLD = 10;
    private static final int NODE_VOCABULARY = 80;

    private static Triple t(int s, int p, int o) {
        return Triple.create(node("s" + s), node("p" + p), node("o" + o));
    }

    private static Node node(String label) {
        return NodeFactory.createURI("http://ex/" + label);
    }

    private static Triple randomTriple(Random rnd) {
        return t(rnd.nextInt(NODE_VOCABULARY),
                 rnd.nextInt(NODE_VOCABULARY / 4),  // fewer predicates → more clustering
                 rnd.nextInt(NODE_VOCABULARY));
    }

    /** A retained published store paired with the exact triple set it held at fork time. */
    private record Held(CowIndexedSetTripleStore store, Set<Triple> expected) {}

    @Test
    public void heldSnapshotsRemainStableAcrossManyCommits() {
        // Deterministic seed so failures are reproducible.
        Random rnd = new Random(0xC0FFEE);

        // Start with a non-empty published store so snapshots have content.
        CowIndexedSetTripleStore published = new CowIndexedSetTripleStore();
        Set<Triple> publishedExpected = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            Triple seed = randomTriple(rnd);
            if (publishedExpected.add(seed)) {
                published.add(seed);
            }
        }

        // Sliding window of held "snapshots" (forks whose original we keep).
        List<Held> heldSnapshots = new ArrayList<>();

        for (int round = 0; round < NUM_ROUNDS; round++) {

            // 1) Verify all currently-held snapshots still match their captured expectation.
            for (Held h : heldSnapshots) {
                assertEquals("size mismatch on held snapshot at round " + round,
                        h.expected.size(), h.store.countTriples());
                // Cheap probes: contains() for a few expected and a few that should be absent.
                int sampleSize = Math.min(8, h.expected.size());
                List<Triple> samples = h.expected.stream().limit(sampleSize).toList();
                for (Triple e : samples) {
                    assertTrue("expected triple missing from held snapshot at round " + round,
                            h.store.contains(e));
                }
                // Stream comparison: full set equality once in a while (expensive).
                if (round % 100 == 0) {
                    Set<Triple> actual = h.store.stream().collect(Collectors.toCollection(HashSet::new));
                    assertEquals("snapshot stream view drifted at round " + round,
                            h.expected, actual);
                }
            }

            // 2) Sometimes hold a new snapshot of the current published state.
            if (heldSnapshots.size() < NUM_SNAPSHOTS_TO_HOLD && rnd.nextInt(3) == 0) {
                heldSnapshots.add(new Held(published, new HashSet<>(publishedExpected)));
                // We cannot mutate `published` again; the next round must
                // start a new write transaction by forking. Replace
                // published with the *fork* (the write target), and add the
                // *original* to heldSnapshots above. After the round's ops
                // and the subsequent "commit", the fork becomes the new
                // published.
                published = published.forkForWrite();
            }

            // 3) Random ops on the writer. (`published` here is currently the
            //    write target — it was just replaced by a fork above when a
            //    snapshot was held; otherwise it's the same instance as last
            //    round, which serves as both publish target and write target
            //    until a snapshot is taken.)
            for (int op = 0; op < OPS_PER_ROUND; op++) {
                Triple x = randomTriple(rnd);
                if (rnd.nextInt(2) == 0) {
                    if (publishedExpected.add(x)) {
                        published.add(x);
                    } else {
                        // Idempotent re-add; expectation already had it.
                        published.add(x);
                    }
                } else {
                    if (publishedExpected.remove(x)) {
                        published.remove(x);
                    } else {
                        published.remove(x);    // no-op, but exercise path
                    }
                }
            }

            // 4) Sanity: writer's view matches our shadow.
            assertEquals("writer view diverged at round " + round,
                    publishedExpected.size(), published.countTriples());
        }

        // Final pass: every held snapshot still reports its captured set.
        for (Held h : heldSnapshots) {
            Set<Triple> actual = h.store.stream().collect(Collectors.toCollection(HashSet::new));
            assertEquals(h.expected, actual);
        }
    }

    /**
     * Targeted property: after a fork, deletes against indices that the
     * snapshot had built up never break the snapshot's index lists or the
     * reverse-index arrays. This is the case where the COW story is
     * trickiest: the writer's swap-with-last on its private reverse array,
     * combined with clone-on-touch on the shared {@link
     * org.apache.jena.mem.store.indexed.IndexList}, must leave the
     * snapshot's view of every still-alive triple intact.
     */
    @Test
    public void deleteHeavyWorkloadDoesNotCorruptSnapshot() {
        Random rnd = new Random(42L);
        CowIndexedSetTripleStore source = new CowIndexedSetTripleStore();
        Set<Triple> seeded = new HashSet<>();
        for (int i = 0; i < 1500; i++) {
            Triple x = randomTriple(rnd);
            if (seeded.add(x)) source.add(x);
        }

        // Capture the snapshot at this point.
        CowIndexedSetTripleStore snapshot = source;
        Set<Triple> expected = new HashSet<>(seeded);

        // Branch a writer and delete most of the seeded triples from it.
        CowIndexedSetTripleStore writer = source.forkForWrite();
        List<Triple> seedList = new ArrayList<>(seeded);
        Collections.shuffle(seedList, rnd);
        for (int i = 0; i < seedList.size() * 9 / 10; i++) {
            writer.remove(seedList.get(i));
        }

        // Add a fresh batch on the writer too.
        for (int i = 0; i < 500; i++) {
            writer.add(randomTriple(rnd));
        }

        // Snapshot is unchanged.
        assertEquals(expected.size(), snapshot.countTriples());
        for (Triple e : expected) {
            assertTrue("snapshot lost a triple after writer deletes/inserts",
                    snapshot.contains(e));
        }
        // Pattern queries on the snapshot still return what they should.
        Set<Triple> snapshotStreamed = snapshot.stream().collect(Collectors.toCollection(HashSet::new));
        assertEquals(expected, snapshotStreamed);

        // Spot-check a SUB_ANY_ANY find against the snapshot for a known seed.
        Triple anySeed = expected.iterator().next();
        assertNotNull(anySeed);
        Set<Triple> bySubject = snapshot.stream(
                Triple.createMatch(anySeed.getSubject(), null, null))
                .collect(Collectors.toCollection(HashSet::new));
        Set<Triple> expectedBySubject = expected.stream()
                .filter(tt -> tt.getSubject().equals(anySeed.getSubject()))
                .collect(Collectors.toCollection(HashSet::new));
        assertEquals(expectedBySubject, bySubject);
    }
}
