package org.apache.jena.mem2.store.kd.tinspin;

import ch.ethz.globis.phtree.PhTree;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Test3D {

    @Test
    public void findForAnyCombination_PH() {
        PhTree<String> tree = PhTree.create(3);
        tree.put(new long[]{0, 0, 0}, "0,0,0");
        tree.put(new long[]{0, 0, 1}, "0,0,1");
        tree.put(new long[]{0, 1, 0}, "0,1,0");
        tree.put(new long[]{0, 1, 1}, "0,1,1");
        tree.put(new long[]{1, 0, 0}, "1,0,0");
        tree.put(new long[]{1, 0, 1}, "1,0,1");
        tree.put(new long[]{1, 1, 0}, "1,1,0");
        tree.put(new long[]{1, 1, 1}, "1,1,1");

        Assert.assertEquals(8, tree.size());

        var q = tree.query(new long[]{0, 0, 0}, new long[]{0, 0, 0});
        Assert.assertTrue(q.hasNext());
        Assert.assertEquals("0,0,0", q.nextValue());

        q = tree.query(new long[]{1, 1, 1}, new long[]{1, 1, 1});
        Assert.assertTrue(q.hasNext());
        Assert.assertEquals("1,1,1", q.nextValue());

        q = tree.query(new long[]{1, 1, 0}, new long[]{1, 1, 1});
        var collection = iteratorToCollection(q);
        Assert.assertEquals(2, collection.size());
        Assert.assertTrue(collection.contains("1,1,0"));
        Assert.assertTrue(collection.contains("1,1,1"));

    }

    private static Collection<String> iteratorToCollection(PhTree.PhQuery<String> q) {
        List<String> list = new ArrayList<>();
        q.forEachRemaining(list::add);
        return list;
    }

    @Test
    public void findForAnyCombination() {
        KDTree<String> tree = KDTree.create(3);
        tree.insert(new int[]{0, 0, 0}, "0,0,0");
        tree.insert(new int[]{0, 0, 1}, "0,0,1");
        tree.insert(new int[]{0, 1, 0}, "0,1,0");
        tree.insert(new int[]{0, 1, 1}, "0,1,1");
        tree.insert(new int[]{1, 0, 0}, "1,0,0");
        tree.insert(new int[]{1, 0, 1}, "1,0,1");
        tree.insert(new int[]{1, 1, 0}, "1,1,0");
        tree.insert(new int[]{1, 1, 1}, "1,1,1");

        var iter = tree.query(new int[]{0, 0, 0}, new int[]{0, 0, 0});
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals("0,0,0", iter.next().value());

        iter = tree.query(new int[]{1, 1, 1}, new int[]{1, 1, 1});
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals("1,1,1", iter.next().value());

        iter = tree.query(new int[]{1, 1, 0}, new int[]{1, 1, 1});
        var collection = nodeIteratorToValueCollection(iter);
        Assert.assertEquals(2, collection.size());
        Assert.assertTrue(collection.contains("1,1,0"));
        Assert.assertTrue(collection.contains("1,1,1"));

    }

    @Test
    public void testDuplicates() {
        KDTree<String> tree = KDTree.create(3);
        tree.insert(new int[]{0, 0, 0}, "0,0,0a");
        tree.insert(new int[]{0, 0, 0}, "0,0,0b");
        tree.insert(new int[]{0, 0, 1}, "0,0,1");
        tree.insert(new int[]{0, 1, 0}, "0,1,0");
        tree.insert(new int[]{0, 1, 1}, "0,1,1");
        tree.insert(new int[]{1, 0, 0}, "1,0,0");
        tree.insert(new int[]{1, 0, 1}, "1,0,1");
        tree.insert(new int[]{1, 1, 0}, "1,1,0");
        tree.insert(new int[]{1, 1, 1}, "1,1,1a");
        tree.insert(new int[]{1, 1, 1}, "1,1,1b");

        var iter = tree.query(new int[]{0, 0, 0}, new int[]{0, 0, 0});
        var collection = nodeIteratorToValueCollection(iter);
        Assert.assertEquals(2, collection.size());
        Assert.assertTrue(collection.contains("0,0,0a"));
        Assert.assertTrue(collection.contains("0,0,0b"));

        iter = tree.query(new int[]{1, 1, 1}, new int[]{1, 1, 1});
        collection = nodeIteratorToValueCollection(iter);
        Assert.assertEquals(2, collection.size());
        Assert.assertTrue(collection.contains("1,1,1a"));
        Assert.assertTrue(collection.contains("1,1,1b"));

        iter = tree.query(new int[]{1, 1, 0}, new int[]{1, 1, 1});
        collection = nodeIteratorToValueCollection(iter);
        Assert.assertEquals(3, collection.size());
        Assert.assertTrue(collection.contains("1,1,0"));
        Assert.assertTrue(collection.contains("1,1,1a"));
        Assert.assertTrue(collection.contains("1,1,1b"));

    }

    @Test
    public void testDuplicates2() {
        KDTree<String> tree = KDTree.create(3);
        tree.insert(new int[]{0, 0, 0}, "0,0,0a");
        tree.insert(new int[]{0, 0, 0}, "0,0,0b");

        var iter = tree.queryExactPoint(new int[]{0, 0, 0});
        var collection = nodeIteratorToValueCollection(iter);
        Assert.assertEquals(2, collection.size());
        Assert.assertTrue(collection.contains("0,0,0a"));
        Assert.assertTrue(collection.contains("0,0,0b"));
    }

    @Test
    public void testContains() {
        KDTree<String> tree = KDTree.create(3);
        tree.insert(new int[]{0, 0, 0}, "0,0,0a");
        tree.insert(new int[]{0, 0, 0}, "0,0,0b");

        Assert.assertTrue(tree.contains(new int[]{0, 0, 0}, "0,0,0a"));
        Assert.assertTrue(tree.contains(new int[]{0, 0, 0}, "0,0,0b"));
        Assert.assertFalse(tree.contains(new int[]{0, 0, 0}, "0,0,0c"));
    }


    @Test
    public void testDuplicates3() {
        KDTree<String> tree = KDTree.create(3);
        tree.insert(new int[]{0, 0, 0}, "0,0,0");
        tree.insert(new int[]{0, 0, 0}, "0,0,0");

        var iter = tree.queryExactPoint(new int[]{0, 0, 0});
        var collection = nodeIteratorToValueCollection(iter);
        Assert.assertEquals(2, collection.size());
        Assert.assertTrue(collection.contains("0,0,0"));
    }

    @Test
    public void testRemove() {
        KDTree<String> tree = KDTree.create(3);
        tree.insert(new int[]{0, 0, 0}, "0,0,0a");
        tree.insert(new int[]{0, 0, 0}, "0,0,0b");
        tree.insert(new int[]{0, 0, 1}, "0,0,1");
        tree.insert(new int[]{0, 1, 0}, "0,1,0");
        tree.insert(new int[]{0, 1, 1}, "0,1,1");
        tree.insert(new int[]{1, 0, 0}, "1,0,0");
        tree.insert(new int[]{1, 0, 1}, "1,0,1");
        tree.insert(new int[]{1, 1, 0}, "1,1,0");
        tree.insert(new int[]{1, 1, 1}, "1,1,1a");
        tree.insert(new int[]{1, 1, 1}, "1,1,1b");

        tree.remove(new int[]{0, 0, 0}, "0,0,0a");

        var iter = tree.query(new int[]{0, 0, 0}, new int[]{0, 0, 0});
        var collection = nodeIteratorToValueCollection(iter);
        Assert.assertEquals(1, collection.size());
        Assert.assertTrue(collection.contains("0,0,0b"));

        tree.remove(new int[]{0, 0, 0}, "0,0,0b");
        iter = tree.query(new int[]{0, 0, 0}, new int[]{0, 0, 0});
        Assert.assertFalse(iter.hasNext());

        tree.remove(new int[]{1, 1, 1}, "1,1,1b");
        iter = tree.query(new int[]{1, 1, 1}, new int[]{1, 1, 1});
        collection = nodeIteratorToValueCollection(iter);
        Assert.assertEquals(1, collection.size());
        Assert.assertTrue(collection.contains("1,1,1a"));
    }

    @Test
    public void testRemove2() {
        KDTree<String> tree = KDTree.create(3);
        tree.insert(new int[]{0, 0, 0}, "0,0,0");
        tree.insert(new int[]{0, 0, 0}, "0,0,0");

        tree.remove(new int[]{0, 0, 0}, "0,0,0");

        var iter = tree.query(new int[]{0, 0, 0}, new int[]{0, 0, 0});
        var collection = nodeIteratorToValueCollection(iter);
        Assert.assertEquals(1, collection.size());
        Assert.assertTrue(collection.contains("0,0,0"));

        tree.remove(new int[]{0, 0, 0}, "0,0,0");
        iter = tree.query(new int[]{0, 0, 0}, new int[]{0, 0, 0});
        Assert.assertFalse(iter.hasNext());
    }

    private static <T> Collection<T> nodeIteratorToValueCollection(KDIterator<T> iter) {
        List<T> list = new ArrayList<>();
        iter.forEachRemaining(pointEntry -> list.add(pointEntry.value()));
        return list;
    }
}
