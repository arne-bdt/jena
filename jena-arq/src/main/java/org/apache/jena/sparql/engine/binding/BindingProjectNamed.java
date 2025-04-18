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

package org.apache.jena.sparql.engine.binding ;

import org.apache.jena.sparql.core.Var ;

/**
 * Project only named variables (i.e. hide variables used because of
 * bNodes or internally generated variables).
 */
public class BindingProjectNamed extends BindingProjectBase {
    public BindingProjectNamed(Binding bind) {
        super(bind) ;
    }

    @Override
    protected boolean accept(Var var) {
        return var.isNamedVar() ;
    }

    @Override
    public Binding detach() {
        Binding b = binding.detach();
        return b == binding
            ? this
            : new BindingProjectNamed(b);
    }

    @Override
    protected Binding detachWithNewParent(Binding newParent) {
        throw new UnsupportedOperationException("Should never be called.");
    }
}
