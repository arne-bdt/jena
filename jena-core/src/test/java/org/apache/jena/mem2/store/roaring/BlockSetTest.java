package org.apache.jena.mem2.store.roaring;

import org.junit.Test;

import static org.apache.jena.testing_framework.GraphHelper.triple;
import static org.junit.Assert.*;

public class BlockSetTest {

    @Test
    public void testBlockSet()
    {
        final var blockSet = new BlockSet();
        for(var i=1;i<=10;i++) {
            blockSet.addAndGetRow(triple("s p o"+i));
            assertEquals(i, blockSet.size());
            assertTrue(blockSet.contains(triple("s p o"+i)));
        }
    }
}
