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

package org.apache.jena.mem2.helper;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.hash_no_entry.ObjectEqualizer;

import java.util.function.Predicate;

public abstract class TripleMatchFilterProvider {

    public static Predicate<Triple> getFilterForTriplePattern(final Node sm, final Node pm, final Node om) {
        if (sm.isConcrete()) { // SPO:S??
            if(pm.isConcrete()) { // SPO:SP?
                if(om.isConcrete()) { // SPO:SPO
                    if(TripleEqualsOrMatches.isEqualsForObjectOk(om)) {
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
                    if(TripleEqualsOrMatches.isEqualsForObjectOk(om)) {
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
            if(TripleEqualsOrMatches.isEqualsForObjectOk(om)) {
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
