/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 *   SPDX-License-Identifier: Apache-2.0
 */

package org.apache.jena.sparql.core.assembler;

import static org.apache.jena.assembler.Mode.DEFAULT;
import static org.apache.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.apache.jena.vocabulary.RDF.type;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import org.apache.jena.assembler.JA;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.mem.DatasetGraphInMemoryMvccTxn;

/** Tests for {@link InMemDatasetMvccAssembler}. */
public class TestInMemDatasetMvccAssembler {

    private Dataset assemble(Resource root) {
        root.getModel().setNsPrefix("ja", JA.getURI());
        InMemDatasetMvccAssembler assembler = new InMemDatasetMvccAssembler();
        return assembler.open(assembler, root, DEFAULT);
    }

    @Test
    public void emptyDataset() {
        Model model = createDefaultModel();
        Resource root = model.createResource("test:empty");
        root.addProperty(type, DatasetAssemblerVocab.tMemoryDatasetMvcc);

        Dataset ds = assemble(root);
        DatasetGraph dsg = assertInstanceOf(DatasetGraphInMemoryMvccTxn.class, ds.asDatasetGraph());
        assertTrue(dsg.supportsTransactions());
        assertFalse(dsg.find().hasNext());
    }
}
