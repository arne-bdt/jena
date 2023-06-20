package org.apache.jena.mem2.collection;

import org.apache.jena.graph.Node;

public class HashCommonMapTest extends AbstractJenaMapNodeTest {

    @Override
    protected JenaMap<Node, Object> createNodeMap() {
        return new HashCommonMap<Node, Object>(10) {
            @Override
            public void clear() {
                super.clear(10);
            }

            @Override
            protected Object[] newValuesArray(int size) {
                return new Object[size];
            }

            @Override
            protected Node[] newKeysArray(int size) {
                return new Node[size];
            }
        };
    }
}