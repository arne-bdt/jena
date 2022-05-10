package org.apache.jena.mem.hash_no_entry;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

public class ObjectEqualizer {

    protected static boolean isEqualsForObjectOk( Triple t )
    {
        return isEqualsForObjectOk(t.getObject());
    }
    protected static boolean isEqualsForObjectOk( Node objectNode )
    {
        if(objectNode.isLiteral() && objectNode.getLiteralDatatype() == null) { /*slow equals*/
            return false;
        }
        return true;
    }
}
