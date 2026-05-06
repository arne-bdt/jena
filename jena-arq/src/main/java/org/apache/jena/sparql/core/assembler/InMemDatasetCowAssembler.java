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

import static org.apache.jena.sparql.core.assembler.AssemblerUtils.loadData;
import static org.apache.jena.sparql.core.assembler.AssemblerUtils.mergeContext;

import java.util.Locale;

import org.apache.jena.assembler.Assembler;
import org.apache.jena.assembler.exceptions.AssemblerException;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.mem.DatasetGraphInMemoryCowTxn;
import org.apache.jena.sparql.core.mem.GraphMemIndexedSetCowTxn;

/**
 * Assembler for {@link DatasetAssemblerVocab#tMemoryDatasetCow} —
 * in-memory, transactional dataset backed by per-graph copy-on-write
 * snapshots ({@link DatasetGraphInMemoryCowTxn}).
 * <p>
 * Optionally reads {@link DatasetAssemblerVocab#pForkMode} as a literal
 * ("SEQUENTIAL" or "PARALLEL", case-insensitive). Defaults to
 * {@link GraphMemIndexedSetCowTxn.ForkMode#SEQUENTIAL}.
 * <p>
 * Like {@link InMemDatasetAssembler}, the dataset can be shared across
 * assemblies by using {@code ja:name}.
 */
public class InMemDatasetCowAssembler extends NamedDatasetAssembler {

    public InMemDatasetCowAssembler() {}

    public static Resource getType() {
        return DatasetAssemblerVocab.tMemoryDatasetCow;
    }

    @Override
    public DatasetGraph createDataset(Assembler a, Resource root) {
        checkType(root, DatasetAssemblerVocab.tMemoryDatasetCow);
        GraphMemIndexedSetCowTxn.ForkMode forkMode = readForkMode(root);
        DatasetGraph dataset = DatasetGraphFactory.createTxnMemCow(forkMode);
        loadData(dataset, root);
        mergeContext(root, dataset.getContext());
        return dataset;
    }

    /**
     * @return the ForkMode named by the {@code ja:forkMode} property if
     * present, otherwise {@link GraphMemIndexedSetCowTxn.ForkMode#SEQUENTIAL}.
     * @throws AssemblerException if the property is present but does not
     * name a valid {@link GraphMemIndexedSetCowTxn.ForkMode}.
     */
    private static GraphMemIndexedSetCowTxn.ForkMode readForkMode(Resource root) {
        Statement st = root.getProperty(DatasetAssemblerVocab.pForkMode);
        if (st == null)
            return GraphMemIndexedSetCowTxn.ForkMode.SEQUENTIAL;
        if (!st.getObject().isLiteral())
            throw new AssemblerException(root,
                    "ja:forkMode must be a literal (\"SEQUENTIAL\" or \"PARALLEL\")");
        String raw = st.getString().trim().toUpperCase(Locale.ROOT);
        try {
            return GraphMemIndexedSetCowTxn.ForkMode.valueOf(raw);
        } catch (IllegalArgumentException ex) {
            throw new AssemblerException(root,
                    "ja:forkMode value '" + st.getString()
                            + "' is not a valid ForkMode (SEQUENTIAL or PARALLEL)");
        }
    }
}
