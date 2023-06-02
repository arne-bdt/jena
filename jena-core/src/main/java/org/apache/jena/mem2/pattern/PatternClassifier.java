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

package org.apache.jena.mem2.pattern;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

/**
 * Classify a triple match into one of the 8 match patterns.
 * <p>
 * The classification is based on the concrete-ness of the subject, predicate and object.
 * A concrete node is one that is not a variable.
 * <p>
 * The classification is used to select the most efficient implementation of a triple store.
 * <p>
 * This is a utility class; there is no need to instantiate it.
 *
 * @see MatchPattern
 */
public class PatternClassifier {

    public static MatchPattern classify  (Triple tripleMatch) {
        if(tripleMatch.getSubject().isConcrete()) {
            if(tripleMatch.getPredicate().isConcrete()) {
                if(tripleMatch.getObject().isConcrete()) {
                    return MatchPattern.SPO;
                } else {
                    return MatchPattern.SP_;
                }
            } else {
                if(tripleMatch.getObject().isConcrete()) {
                    return MatchPattern.S_O;
                } else {
                    return MatchPattern.S__;
                }
            }
        } else {
            if(tripleMatch.getPredicate().isConcrete()) {
                if(tripleMatch.getObject().isConcrete()) {
                    return MatchPattern._PO;
                } else {
                    return MatchPattern._P_;
                }
            } else {
                if(tripleMatch.getObject().isConcrete()) {
                    return MatchPattern.__O;
                } else {
                    return MatchPattern.___;
                }
            }
        }

    }

    public static MatchPattern classify  (Node sm, Node pm, Node om) {
        if(null != sm && sm.isConcrete()) {
            if(null != pm && pm.isConcrete()) {
                if(null != om && om.isConcrete()) {
                    return MatchPattern.SPO;
                } else {
                    return MatchPattern.SP_;
                }
            } else {
                if(null != om && om.isConcrete()) {
                    return MatchPattern.S_O;
                } else {
                    return MatchPattern.S__;
                }
            }
        } else {
            if(null != pm && pm.isConcrete()) {
                if(null != om && om.isConcrete()) {
                    return MatchPattern._PO;
                } else {
                    return MatchPattern._P_;
                }
            } else {
                if(null != om && om.isConcrete()) {
                    return MatchPattern.__O;
                } else {
                    return MatchPattern.___;
                }
            }
        }

    }
}
