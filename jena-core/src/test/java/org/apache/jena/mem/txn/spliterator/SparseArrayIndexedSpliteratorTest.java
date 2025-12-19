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
package org.apache.jena.mem.txn.spliterator;

import org.apache.jena.mem.txn.collection.IndexedKey;
import org.junit.Test;

import java.util.ArrayList;

import static java.util.Spliterator.*;
import static org.junit.Assert.*;

public class SparseArrayIndexedSpliteratorTest {

    @Test
    public void tryAdvanceEmpty() {
        {
            final var array = new String[0];
            final var deleted = new boolean[0];
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            assertFalse(spliterator.tryAdvance(i -> fail("Should not have advanced")));
        }
        {
            final var array = new String[1];
            final var deleted = new boolean[] { true };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            assertFalse(spliterator.tryAdvance(i -> fail("Should not have advanced")));
        }
    }

    @Test
    public void tryAdvanceOne() {
        {
            final var array = new String[]{"a"};
            final var deleted = new boolean[] { false };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            for(int i=0; i<array.length; i++) {
                final var index = i;
                assertTrue(spliterator.tryAdvance(entry -> {
                    assertEquals(index, entry.index());
                    assertEquals(array[index], entry.key());
                }));
            }
            assertFalse(spliterator.tryAdvance(i -> fail("Should not have advanced")));
        }
        {
            final var array = new String[]{"a", null};
            final var deleted = new boolean[] { false, true };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            final var itemsFound = new ArrayList<>();
            while (true) {
                if (!spliterator.tryAdvance(entry -> itemsFound.add(entry.key()))) {
                    break;
                }
            }
            assertEquals(1, itemsFound.size());
            assertTrue(itemsFound.contains("a"));
        }
        {
            final var array = new String[]{null, "a"};
            final var deleted = new boolean[] { true, false };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            assertTrue(spliterator.tryAdvance(entry -> {
                assertEquals(1, entry.index());
                assertEquals("a", entry.key());
            }));
            assertFalse(spliterator.tryAdvance(i -> fail("Should not have advanced")));
        }
    }

    @Test
    public void tryAdvanceTwo() {
        {
            final var array = new String[]{"a", "b"};
            final var deleted = new boolean[] { false, false };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            final var keysFound = new ArrayList<>();
            final var indicesFound = new ArrayList<>();
            while (spliterator.tryAdvance(i -> {
                keysFound.add(i.key());
                indicesFound.add(i.index());
            }));
            assertEquals(2, keysFound.size());
            assertEquals("a", keysFound.get(0));
            assertEquals("b", keysFound.get(1));
            assertEquals(0, indicesFound.get(0));
            assertEquals(1, indicesFound.get(1));
        }
        {
            final var array = new String[]{"a", null, "b"};
            final var deleted = new boolean[] { false, true, false };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            final var keysFound = new ArrayList<>();
            final var indicesFound = new ArrayList<>();
            while (spliterator.tryAdvance(i -> {
                keysFound.add(i.key());
                indicesFound.add(i.index());
            }));
            assertEquals(2, keysFound.size());
            assertEquals("a", keysFound.get(0));
            assertEquals("b", keysFound.get(1));
            assertEquals(0, indicesFound.get(0));
            assertEquals(2, indicesFound.get(1));
        }
        {
            final var array = new String[]{"a", null, null, "b"};
            final var deleted = new boolean[] { false, true, true, false };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            final var keysFound = new ArrayList<>();
            final var indicesFound = new ArrayList<>();
            while (spliterator.tryAdvance(i -> {
                keysFound.add(i.key());
                indicesFound.add(i.index());
            }));
            assertEquals(2, keysFound.size());
            assertEquals("a", keysFound.get(0));
            assertEquals("b", keysFound.get(1));
            assertEquals(0, indicesFound.get(0));
            assertEquals(3, indicesFound.get(1));
        }
        {
            final var array = new String[]{null, "a", null, "b"};
            final var deleted = new boolean[] { true, false, true, false };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            final var keysFound = new ArrayList<>();
            final var indicesFound = new ArrayList<>();
            while (spliterator.tryAdvance(i -> {
                keysFound.add(i.key());
                indicesFound.add(i.index());
            }));
            assertEquals(2, keysFound.size());
            assertEquals("a", keysFound.get(0));
            assertEquals("b", keysFound.get(1));
            assertEquals(1, indicesFound.get(0));
            assertEquals(3, indicesFound.get(1));
        }
        {
            final var array = new String[]{null, "a", null, null, "b"};
            final var deleted = new boolean[] { true, false, true, true, false };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            final var keysFound = new ArrayList<>();
            final var indicesFound = new ArrayList<>();
            while (spliterator.tryAdvance(i -> {
                keysFound.add(i.key());
                indicesFound.add(i.index());
            }));
            assertEquals(2, keysFound.size());
            assertEquals("a", keysFound.get(0));
            assertEquals("b", keysFound.get(1));
            assertEquals(1, indicesFound.get(0));
            assertEquals(4, indicesFound.get(1));
        }
    }

    @Test
    public void tryAdvanceThree() {
        {
            final var array = new String[]{"a", "b", "c"};
            final var deleted = new boolean[] { false, false, false };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            final var keysFound = new ArrayList<>();
            final var indicesFound = new ArrayList<>();
            while (spliterator.tryAdvance(i -> {
                keysFound.add(i.key());
                indicesFound.add(i.index());
            }));
            assertEquals(3, keysFound.size());
            assertEquals("a", keysFound.get(0));
            assertEquals("b", keysFound.get(1));
            assertEquals("c", keysFound.get(2));
            assertEquals(0, indicesFound.get(0));
            assertEquals(1, indicesFound.get(1));
            assertEquals(2, indicesFound.get(2));
        }
        {
            final var array = new String[]{"a", null, "b", "c"};
            final var deleted = new boolean[] { false, true, false, false };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            final var keysFound = new ArrayList<>();
            final var indicesFound = new ArrayList<>();
            while (spliterator.tryAdvance(i -> {
                keysFound.add(i.key());
                indicesFound.add(i.index());
            }));
            assertEquals(3, keysFound.size());
            assertEquals("a", keysFound.get(0));
            assertEquals("b", keysFound.get(1));
            assertEquals("c", keysFound.get(2));
            assertEquals(0, indicesFound.get(0));
            assertEquals(2, indicesFound.get(1));
            assertEquals(3, indicesFound.get(2));
        }
        {
            final var array = new String[]{"a", null, null, "b", "c"};
            final var deleted = new boolean[] { false, true, true, false, false };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            final var keysFound = new ArrayList<>();
            final var indicesFound = new ArrayList<>();
            while (spliterator.tryAdvance(i -> {
                keysFound.add(i.key());
                indicesFound.add(i.index());
            }));
            assertEquals(3, keysFound.size());
            assertEquals("a", keysFound.get(0));
            assertEquals("b", keysFound.get(1));
            assertEquals("c", keysFound.get(2));
            assertEquals(0, indicesFound.get(0));
            assertEquals(3, indicesFound.get(1));
            assertEquals(4, indicesFound.get(2));
        }
        {
            final var array = new String[]{null, "a", null, "b", null, "c"};
            final var deleted = new boolean[] { true, false, true, false, true, false };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            final var keysFound = new ArrayList<>();
            final var indicesFound = new ArrayList<>();
            while (spliterator.tryAdvance(i -> {
                keysFound.add(i.key());
                indicesFound.add(i.index());
            }));
            assertEquals(3, keysFound.size());
            assertEquals("a", keysFound.get(0));
            assertEquals("b", keysFound.get(1));
            assertEquals("c", keysFound.get(2));
            assertEquals(1, indicesFound.get(0));
            assertEquals(3, indicesFound.get(1));
            assertEquals(5, indicesFound.get(2));
        }
        {
            final var array = new String[]{null, "a", null, null, "b", null, "c"};
            final var deleted = new boolean[] { true, false, true, true, false, true, false };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            final var keysFound = new ArrayList<>();
            final var indicesFound = new ArrayList<>();
            while (spliterator.tryAdvance(i -> {
                keysFound.add(i.key());
                indicesFound.add(i.index());
            }));
            assertEquals(3, keysFound.size());
            assertEquals("a", keysFound.get(0));
            assertEquals("b", keysFound.get(1));
            assertEquals("c", keysFound.get(2));
            assertEquals(1, indicesFound.get(0));
            assertEquals(4, indicesFound.get(1));
            assertEquals(6, indicesFound.get(2));
        }
    }

    @Test
    public void forEachRemainingEmpty() {
        {
            final var array = new String[]{};
            final var deleted = new boolean[] {};
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            var itemsFound = new ArrayList<IndexedKey<String>>();
            spliterator.forEachRemaining(itemsFound::add);
            assertEquals(0, itemsFound.size());
        }
        {
            final var array = new String[]{null};
            final var deleted = new boolean[] { true };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            var itemsFound = new ArrayList<>();
            spliterator.forEachRemaining(itemsFound::add);
            assertEquals(0, itemsFound.size());
        }
    }

    @Test
    public void forEachRemainingOne() {
        {
            final var array = new String[]{"a"};
            final var deleted = new boolean[] { false };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            final var keysFound = new ArrayList<>();
            final var indicesFound = new ArrayList<>();
            spliterator.forEachRemaining(entry -> {
                keysFound.add(entry.key());
                indicesFound.add(entry.index());
            });
            assertEquals(1, keysFound.size());
            assertTrue(keysFound.contains("a"));
            assertTrue(indicesFound.contains(0));
        }
        {
            final var array = new String[]{null, "a"};
            final var deleted = new boolean[] { true, false };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            final var keysFound = new ArrayList<>();
            final var indicesFound = new ArrayList<>();
            spliterator.forEachRemaining(entry -> {
                keysFound.add(entry.key());
                indicesFound.add(entry.index());
            });
            assertEquals(1, keysFound.size());
            assertTrue(keysFound.contains("a"));
            assertTrue(indicesFound.contains(1));
        }
        {
            final var array = new String[]{"a", null};
            final var deleted = new boolean[] { false, true };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            final var keysFound = new ArrayList<>();
            final var indicesFound = new ArrayList<>();
            spliterator.forEachRemaining(entry -> {
                keysFound.add(entry.key());
                indicesFound.add(entry.index());
            });
            assertEquals(1, keysFound.size());
            assertTrue(keysFound.contains("a"));
            assertTrue(indicesFound.contains(0));
        }
    }

    @Test
    public void forEachRemainingTwo() {
        {
            final var array = new String[]{"a", "b"};
            final var deleted = new boolean[] { false, false };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            final var keysFound = new ArrayList<>();
            final var indicesFound = new ArrayList<>();
            spliterator.forEachRemaining(entry -> {
                keysFound.add(entry.key());
                indicesFound.add(entry.index());
            });
            assertEquals(2, keysFound.size());
            assertEquals("a", keysFound.get(0));
            assertEquals("b", keysFound.get(1));
            assertEquals(0, indicesFound.get(0));
            assertEquals(1, indicesFound.get(1));
        }
        {
            final var array = new String[]{"a", null, "b"};
            final var deleted = new boolean[] { false, true, false };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            final var keysFound = new ArrayList<>();
            final var indicesFound = new ArrayList<>();
            spliterator.forEachRemaining(entry -> {
                keysFound.add(entry.key());
                indicesFound.add(entry.index());
            });
            assertEquals(2, keysFound.size());
            assertEquals("a", keysFound.get(0));
            assertEquals("b", keysFound.get(1));
            assertEquals(0, indicesFound.get(0));
            assertEquals(2, indicesFound.get(1));
        }
        {
            final var array = new String[]{"a", null, null, "b"};
            final var deleted = new boolean[] { false, true, true, false };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            final var keysFound = new ArrayList<>();
            final var indicesFound = new ArrayList<>();
            spliterator.forEachRemaining(entry -> {
                keysFound.add(entry.key());
                indicesFound.add(entry.index());
            });
            assertEquals(2, keysFound.size());
            assertEquals("a", keysFound.get(0));
            assertEquals("b", keysFound.get(1));
            assertEquals(0, indicesFound.get(0));
            assertEquals(3, indicesFound.get(1));
        }
        {
            final var array = new String[]{null, "a", null, "b"};
            final var deleted = new boolean[] { true, false, true, false };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            final var keysFound = new ArrayList<>();
            final var indicesFound = new ArrayList<>();
            spliterator.forEachRemaining(entry -> {
                keysFound.add(entry.key());
                indicesFound.add(entry.index());
            });
            assertEquals(2, keysFound.size());
            assertEquals("a", keysFound.get(0));
            assertEquals("b", keysFound.get(1));
            assertEquals(1, indicesFound.get(0));
            assertEquals(3, indicesFound.get(1));
        }
        {
            final var array = new String[]{null, "a", null, null, "b"};
            final var deleted = new boolean[] { true, false, true, true, false };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            final var keysFound = new ArrayList<>();
            final var indicesFound = new ArrayList<>();
            spliterator.forEachRemaining(entry -> {
                keysFound.add(entry.key());
                indicesFound.add(entry.index());
            });
            assertEquals(2, keysFound.size());
            assertEquals("a", keysFound.get(0));
            assertEquals("b", keysFound.get(1));
            assertEquals(1, indicesFound.get(0));
            assertEquals(4, indicesFound.get(1));
        }
    }

    @Test
    public void forEachRemainingThree() {
        {
            final var array = new String[]{"a", "b", "c"};
            final var deleted = new boolean[] { false, false, false };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            final var keysFound = new ArrayList<>();
            final var indicesFound = new ArrayList<>();
            spliterator.forEachRemaining(entry -> {
                keysFound.add(entry.key());
                indicesFound.add(entry.index());
            });
            assertEquals(3, keysFound.size());
            assertEquals("a", keysFound.get(0));
            assertEquals("b", keysFound.get(1));
            assertEquals("c", keysFound.get(2));
            assertEquals(0, indicesFound.get(0));
            assertEquals(1, indicesFound.get(1));
            assertEquals(2, indicesFound.get(2));
        }
        {
            final var array = new String[]{"a", null, "b", "c"};
            final var deleted = new boolean[] { false, true, false, false };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            final var keysFound = new ArrayList<>();
            final var indicesFound = new ArrayList<>();
            spliterator.forEachRemaining(entry -> {
                keysFound.add(entry.key());
                indicesFound.add(entry.index());
            });
            assertEquals(3, keysFound.size());
            assertEquals("a", keysFound.get(0));
            assertEquals("b", keysFound.get(1));
            assertEquals("c", keysFound.get(2));
            assertEquals(0, indicesFound.get(0));
            assertEquals(2, indicesFound.get(1));
            assertEquals(3, indicesFound.get(2));
        }
        {
            final var array = new String[]{"a", null, null, "b", "c"};
            final var deleted = new boolean[] { false, true, true, false, false };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            final var keysFound = new ArrayList<>();
            final var indicesFound = new ArrayList<>();
            spliterator.forEachRemaining(entry -> {
                keysFound.add(entry.key());
                indicesFound.add(entry.index());
            });
            assertEquals(3, keysFound.size());
            assertEquals("a", keysFound.get(0));
            assertEquals("b", keysFound.get(1));
            assertEquals("c", keysFound.get(2));
            assertEquals(0, indicesFound.get(0));
            assertEquals(3, indicesFound.get(1));
            assertEquals(4, indicesFound.get(2));
        }
        {
            final var array = new String[]{null, "a", null, "b", "c"};
            final var deleted = new boolean[] { true, false, true, false, false };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            final var keysFound = new ArrayList<>();
            final var indicesFound = new ArrayList<>();
            spliterator.forEachRemaining(entry -> {
                keysFound.add(entry.key());
                indicesFound.add(entry.index());
            });
            assertEquals(3, keysFound.size());
            assertEquals("a", keysFound.get(0));
            assertEquals("b", keysFound.get(1));
            assertEquals("c", keysFound.get(2));
            assertEquals(1, indicesFound.get(0));
            assertEquals(3, indicesFound.get(1));
            assertEquals(4, indicesFound.get(2));
        }
        {
            final var array = new String[]{null, "a", null, null, "b", "c"};
            final var deleted = new boolean[] { true, false, true, true, false, false };
            final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
            final var keysFound = new ArrayList<>();
            final var indicesFound = new ArrayList<>();
            spliterator.forEachRemaining(entry -> {
                keysFound.add(entry.key());
                indicesFound.add(entry.index());
            });
            assertEquals(3, keysFound.size());
            assertEquals("a", keysFound.get(0));
            assertEquals("b", keysFound.get(1));
            assertEquals("c", keysFound.get(2));
            assertEquals(1, indicesFound.get(0));
            assertEquals(4, indicesFound.get(1));
            assertEquals(5, indicesFound.get(2));
        }
    }

    @Test
    public void trySplitEmpty() {
        final var array = new String[]{};
        final var deleted = new boolean[] {};
        final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
        assertNull(spliterator.trySplit());
    }

    @Test
    public void trySplitOne() {
        final var array = new String[]{"a"};
        final var deleted = new boolean[] { false };
        final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
        assertNull(spliterator.trySplit());
    }

    @Test
    public void trySplitTwo() {
        final var array = new String[]{"a", "b"};
        final var deleted = new boolean[] { false, false };
        final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
        // Estimated size is not exact
        assertBetween(2, 3, spliterator.estimateSize());
        final var split = spliterator.trySplit();
        assertBetween(1, 2, spliterator.estimateSize());
        assertBetween(1, 3, split.estimateSize());
    }

    @Test
    public void trySplitThree() {
        final var array = new String[]{"a", "b", "c"};
        final var deleted = new boolean[] { false, false, false };
        final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
        // Estimated size is not exact
        assertBetween(3, 4, spliterator.estimateSize());
        final var split = spliterator.trySplit();
        assertBetween(1, 3, spliterator.estimateSize());
        assertBetween(2, 3, split.estimateSize());
    }

    @Test
    public void trySplitFour() {
        final var array = new String[]{"a", "b", "c", "d"};
        final var deleted = new boolean[] { false, false, false, false };
        final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
        // Estimated size is not exact
        assertBetween(4, 5, spliterator.estimateSize());
        final var split = spliterator.trySplit();
        assertBetween(2, 3, spliterator.estimateSize());
        assertBetween(2, 4, split.estimateSize());
    }

    @Test
    public void trySplitFive() {
        final var array = new String[]{"a", "b", "c", "d", "e"};
        final var deleted = new boolean[] { false, false, false, false, false };
        final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
        // Estimated size is not exact
        assertBetween(5, 6, spliterator.estimateSize());
        final var split = spliterator.trySplit();
        assertBetween(2, 4, spliterator.estimateSize());
        assertBetween(2, 4, split.estimateSize());

        final var keysFound = new ArrayList<>();
        final var indicesFound = new ArrayList<>();
        split.forEachRemaining(entry -> {
            keysFound.add(entry.key());
            indicesFound.add(entry.index());
        });
        spliterator.forEachRemaining(entry -> {
            keysFound.add(entry.key());
            indicesFound.add(entry.index());
        });
        assertEquals(5, keysFound.size());
        assertEquals("a", keysFound.get(0));
        assertEquals("b", keysFound.get(1));
        assertEquals("c", keysFound.get(2));
        assertEquals("d", keysFound.get(3));
        assertEquals("e", keysFound.get(4));
        assertEquals(0, indicesFound.get(0));
        assertEquals(1, indicesFound.get(1));
        assertEquals(2, indicesFound.get(2));
        assertEquals(3, indicesFound.get(3));
        assertEquals(4, indicesFound.get(4));
    }

    @Test
    public void trySplitOneHundred() {
        Integer[] array = new Integer[200];
        final var deleted = new boolean[array.length];
        for (int i = 0; i < array.length; i++) {
            if (i % 2 == 0) {
                array[i] = i;
            } else {
                deleted[i] = true;
            }
        }
        final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
        // Estimated size is not exact
        assertBetween(array.length, array.length+1, spliterator.estimateSize());
        final var split = spliterator.trySplit();
        assertBetween(array.length / 2, (array.length / 2) + 1, spliterator.estimateSize());
        assertBetween(array.length / 2, (array.length / 2) + 1, split.estimateSize());
    }

    private void assertBetween(long min, long max, long estimateSize) {
        assertTrue("estimateSize=" + estimateSize + " min=" + min + " max=" + max, estimateSize >= min);
        assertTrue("estimateSize=" + estimateSize + " min=" + min + " max=" + max, estimateSize <= max);
    }

    @Test
    public void estimateSizeZero() {
        final var array = new String[]{};
        final var deleted = new boolean[] {};
        final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
        assertBetween(0, 1, spliterator.estimateSize());
    }

    @Test
    public void estimateSizeOne() {
        final var array = new String[]{"a"};
        final var deleted = new boolean[] { false };
        final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
        assertBetween(1, 2, spliterator.estimateSize());
    }

    @Test
    public void estimateSizeTwo() {
        final var array = new String[]{"a", "b"};
        final var deleted = new boolean[] { false, false };
        final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
        assertBetween(2, 3, spliterator.estimateSize());
    }

    @Test
    public void estimateSizeFive() {
        final var array = new String[]{"a", "b", "c", "d", "e"};
        final var deleted = new boolean[] { false, false, false, false, false };
        final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
        assertBetween(5, 6, spliterator.estimateSize());
    }

    @Test
    public void characteristics() {
        final var array = new String[]{"a", "b", "c", "d", "e"};
        final var deleted = new boolean[] { false, false, false, false, false };
        final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
        assertEquals(DISTINCT | NONNULL | IMMUTABLE, spliterator.characteristics());
    }

    @Test
    public void splitWithOneElementNull() {
        final var array = new String[]{null};
        final var deleted = new boolean[] { true };
        final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
        assertNull(spliterator.trySplit());
    }

    @Test
    public void splitWithOneRemainingElementNull() {
        final var array = new String[]{"a", null};
        final var deleted = new boolean[] { false, true };
        final var spliterator = new SparseArrayIndexedSpliterator<>(array, deleted, () -> {});
        spliterator.tryAdvance(i -> {});
        assertNull(spliterator.trySplit());
    }
}