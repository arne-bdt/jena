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

import org.apache.jena.assembler.Assembler;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.mem.DatasetGraphInMemoryMvccTxn;

/**
 * Assembler for {@link DatasetAssemblerVocab#tMemoryDatasetMvcc} — an in-memory,
 * transactional dataset backed by an MVCC (version-stamped) shared store
 * ({@link DatasetGraphInMemoryMvccTxn}).
 * <p>
 * Like {@link InMemDatasetAssembler} and {@link InMemDatasetCowAssembler}, the
 * dataset can be shared across assemblies by using {@code ja:name}.
 */
public class InMemDatasetMvccAssembler extends NamedDatasetAssembler {

    public InMemDatasetMvccAssembler() {}

    public static Resource getType() {
        return DatasetAssemblerVocab.tMemoryDatasetMvcc;
    }

    @Override
    public DatasetGraph createDataset(Assembler a, Resource root) {
        checkType(root, DatasetAssemblerVocab.tMemoryDatasetMvcc);
        DatasetGraph dataset = DatasetGraphFactory.createTxnMemMvcc();
        loadData(dataset, root);
        mergeContext(root, dataset.getContext());
        return dataset;
    }
}
