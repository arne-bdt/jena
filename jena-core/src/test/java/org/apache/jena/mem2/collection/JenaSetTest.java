package org.apache.jena.mem2.collection;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.store.fast.FastArrayBunch;
import org.apache.jena.mem2.store.legacy.ArrayBunch;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.apache.jena.testing_framework.GraphHelper.triple;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class JenaSetTest {

    final Class<JenaSet<Triple>> setClass;
    JenaSet<Triple> sut;

    public JenaSetTest(String className, Class<JenaSet<Triple>> setClass) {
        this.setClass = setClass;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection setImplementations() {
        return Arrays.asList(new Object[][]{
                {HashCommonSet.class.getName(), HashCommonTripleSet.class},
                {FastHashSet.class.getName(), FastTripleHashSet.class},
                {ArrayBunch.class.getName(), ArrayBunch.class},
                {FastArrayBunch.class.getName(), FastArrayTripleBunch.class},
        });
    }

    @Before
    public void setUp() throws Exception {
        sut = setClass.newInstance();
    }

    @Test
    public void isEmpty() {
        assertTrue(sut.isEmpty());
        sut.tryAdd(triple("s o p"));
        assertFalse(sut.isEmpty());
    }

    @Test
    public void testTryAdd() {
        assertTrue(sut.tryAdd(triple("s o p")));
        assertFalse(sut.tryAdd(triple("s o p")));
        assertEquals(sut.size(), 1);
    }

    private static class HashCommonTripleSet extends HashCommonSet<Triple> {
        public HashCommonTripleSet() {
            super(10);
        }

        @Override
        protected Triple[] newKeysArray(int size) {
            return new Triple[size];
        }

        @Override
        public void clear() {
            super.clear(10);
        }
    }

    private static class FastTripleHashSet extends FastHashSet<Triple> {
        @Override
        protected Triple[] newKeysArray(int size) {
            return new Triple[size];
        }
    }

    private static class FastArrayTripleBunch extends FastArrayBunch {
        @Override
        public boolean areEqual(Triple a, Triple b) {
            return a.equals(b);
        }
    }


}