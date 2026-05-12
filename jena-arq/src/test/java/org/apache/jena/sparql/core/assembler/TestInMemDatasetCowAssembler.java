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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import org.apache.jena.assembler.JA;
import org.apache.jena.assembler.exceptions.AssemblerException;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.TxnType;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.mem.DatasetGraphInMemoryCowTxn;
import org.apache.jena.sparql.core.mem.GraphMemIndexedSetCowTxn;
import org.apache.jena.sparql.sse.SSE;

/**
 * Tests for {@link InMemDatasetCowAssembler}: dataset construction,
 * default ForkMode, explicit ForkMode parsing, and error handling for
 * malformed ja:forkMode values.
 */
public class TestInMemDatasetCowAssembler {

    private Dataset assemble(Resource root) {
        root.getModel().setNsPrefix("ja", JA.getURI());
        InMemDatasetCowAssembler assembler = new InMemDatasetCowAssembler();
        return assembler.open(assembler, root, DEFAULT);
    }

    private static DatasetGraphInMemoryCowTxn unwrap(Dataset ds) {
        return assertInstanceOf(DatasetGraphInMemoryCowTxn.class, ds.asDatasetGraph());
    }

    @Test
    public void emptyDatasetWithDefaultForkMode() {
        Model model = createDefaultModel();
        Resource root = model.createResource("test:empty");
        root.addProperty(type, DatasetAssemblerVocab.tMemoryDatasetCow);

        Dataset ds = assemble(root);
        DatasetGraphInMemoryCowTxn cow = unwrap(ds);
        assertEquals(GraphMemIndexedSetCowTxn.ForkMode.SEQUENTIAL, cow.getForkMode());
        assertFalse(cow.find().hasNext());
    }

    @Test
    public void forkModeSequentialExplicit() {
        Model model = createDefaultModel();
        Resource root = model.createResource("test:seq");
        root.addProperty(type, DatasetAssemblerVocab.tMemoryDatasetCow);
        root.addProperty(DatasetAssemblerVocab.pForkMode, "SEQUENTIAL");

        Dataset ds = assemble(root);
        assertEquals(GraphMemIndexedSetCowTxn.ForkMode.SEQUENTIAL, unwrap(ds).getForkMode());
    }

    @Test
    public void forkModeParallel() {
        Model model = createDefaultModel();
        Resource root = model.createResource("test:par");
        root.addProperty(type, DatasetAssemblerVocab.tMemoryDatasetCow);
        root.addProperty(DatasetAssemblerVocab.pForkMode, "PARALLEL");

        Dataset ds = assemble(root);
        assertEquals(GraphMemIndexedSetCowTxn.ForkMode.PARALLEL, unwrap(ds).getForkMode());
    }

    @Test
    public void forkModeIsCaseInsensitive() {
        Model model = createDefaultModel();
        Resource root = model.createResource("test:par-lc");
        root.addProperty(type, DatasetAssemblerVocab.tMemoryDatasetCow);
        root.addProperty(DatasetAssemblerVocab.pForkMode, "parallel");

        Dataset ds = assemble(root);
        assertEquals(GraphMemIndexedSetCowTxn.ForkMode.PARALLEL, unwrap(ds).getForkMode());
    }

    @Test
    public void unknownForkModeRejected() {
        Model model = createDefaultModel();
        Resource root = model.createResource("test:bad");
        root.addProperty(type, DatasetAssemblerVocab.tMemoryDatasetCow);
        root.addProperty(DatasetAssemblerVocab.pForkMode, "FAST");

        assertThrows(AssemblerException.class, () -> assemble(root));
    }

    @Test
    public void nonLiteralForkModeRejected() {
        Model model = createDefaultModel();
        Resource root = model.createResource("test:bad-res");
        root.addProperty(type, DatasetAssemblerVocab.tMemoryDatasetCow);
        root.addProperty(DatasetAssemblerVocab.pForkMode,
                model.createResource("test:somewhere"));

        assertThrows(AssemblerException.class, () -> assemble(root));
    }

    /**
     * Sanity: the assembled dataset accepts writes through the standard
     * Transactional API and round-trips them. Catches any wiring mistake
     * without duplicating the contract suite in {@code TestDatasetGraphInMemoryCowTxn*}.
     */
    @Test
    public void assembledDatasetSupportsTransactionalRoundTrip() {
        Model model = createDefaultModel();
        Resource root = model.createResource("test:rt");
        root.addProperty(type, DatasetAssemblerVocab.tMemoryDatasetCow);

        Dataset ds = assemble(root);
        DatasetGraph dsg = ds.asDatasetGraph();
        assertTrue(dsg.supportsTransactions());
        assertTrue(dsg.supportsTransactionAbort());

        Quad q = SSE.parseQuad("(:g :s :p :o)");
        dsg.begin(TxnType.WRITE);
        try {
            dsg.add(q);
            dsg.commit();
        } finally {
            dsg.end();
        }
        dsg.begin(TxnType.READ);
        try {
            assertTrue(dsg.contains(q));
        } finally {
            dsg.end();
        }
    }
}
