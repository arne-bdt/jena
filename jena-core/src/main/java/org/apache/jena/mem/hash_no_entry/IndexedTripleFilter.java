package org.apache.jena.mem.hash_no_entry;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

import java.util.function.Predicate;

public abstract class IndexedTripleFilter {

    public static Predicate<Triple> getFilterForTriplePattern(final Node sm, final Node pm, final Node om) {
        if (sm.isConcrete()) { // SPO:S??
            if(pm.isConcrete()) { // SPO:SP?
                if(om.isConcrete()) { // SPO:SPO
                    if(ObjectEqualizer.isEqualsForObjectOk(om)) {
                        return t -> pm.equals(t.getPredicate()) /*check predicate first because it is not indexed*/
                                && om.equals(t.getObject())     /*check object second because it might not be indexed*/
                                && sm.equals(t.getSubject());   /*check subject last because it is indexed*/
                    } else {
                        return t -> pm.equals(t.getPredicate())
                                && om.sameValueAs(t.getObject())
                                && sm.equals(t.getSubject());
                    }
                } else { // SPO:SP*
                    return t -> sm.equals(t.getSubject())
                            && pm.equals(t.getPredicate());
                }
            } else { // SPO:S*?
                if(om.isConcrete()) { // SPO:S*O
                    if(ObjectEqualizer.isEqualsForObjectOk(om)) {
                        return t -> om.equals(t.getObject())
                                && sm.equals(t.getSubject());
                    } else {
                        return t -> om.sameValueAs(t.getObject())
                                && sm.equals(t.getSubject());
                    }
                } else { // SPO:S**
                    return t -> sm.equals(t.getSubject());
                }
            }
        }
        else if (om.isConcrete()) { // SPO:*?O
            if(ObjectEqualizer.isEqualsForObjectOk(om)) {
                if (pm.isConcrete()) { // SPO:*PO
                    return t -> pm.equals(t.getPredicate()) /*check predicate first because it is not indexed*/
                            && om.equals(t.getObject());    /*check predicate first because it is indexed*/
                } else { // SPO:**O
                    return t -> om.equals(t.getObject());
                }
            } else {
                if (pm.isConcrete()) { // SPO:*PO
                    return t -> pm.equals(t.getPredicate()) /*check predicate first because it is not indexed*/
                            && om.sameValueAs(t.getObject());    /*check predicate first because it is indexed*/
                } else { // SPO:**O
                    return t -> om.sameValueAs(t.getObject());
                }
            }
        }
        else if (pm.isConcrete()) { // SPO:*P*
            return t -> pm.equals(t.getPredicate());
        }
        else { // SPO:***
            return t -> true;
        }
    }
}
