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

package org.apache.jena.riot.lang.cimxml.query;

import org.apache.jena.graph.*;
import org.apache.jena.riot.lang.cimxml.CIMHeaderVocabulary;
import org.apache.jena.riot.lang.cimxml.CIMXMLDocumentContext;
import org.apache.jena.riot.lang.cimxml.graph.ModelHeader;
import org.apache.jena.sparql.core.DatasetGraph;

public interface CIMDatasetGraph extends DatasetGraph {

    default boolean isFullModel() {
        return containsGraph(CIMHeaderVocabulary.TYPE_FULL_MODEL);
    }

    default boolean isDifferenceModel() {
        return containsGraph(CIMHeaderVocabulary.TYPE_DIFFERENCE_MODEL);
    }

    default Graph getForwardDifferences() {
        if(!this.isDifferenceModel())
            throw new IllegalStateException("Forward differences are only available for DifferenceModels. Use isDifferentModel() to check.");
        return getGraph(CIMHeaderVocabulary.GRAPH_FORWARD_DIFFERENCES);
    }

    default Graph getReverseDifferences() {
        if(!this.isDifferenceModel())
            throw new IllegalStateException("Reverse differences are only available for DifferenceModels. Use isDifferentModel() to check.");
        return getGraph(CIMHeaderVocabulary.GRAPH_REVERSE_DIFFERENCES);
    }

    default Graph getPreconditions() {
        if(!this.isDifferenceModel())
            throw new IllegalStateException("Preconditions are only available for DifferenceModels. Use isDifferentModel() to check.");
        return getGraph(CIMHeaderVocabulary.GRAPH_PRECONDITIONS);
    }

    default ModelHeader getModelHeader() {
        var graphName = isFullModel()
                ? CIMHeaderVocabulary.TYPE_FULL_MODEL
                : isDifferenceModel()
                    ? CIMHeaderVocabulary.TYPE_DIFFERENCE_MODEL
                    : null;

        if(graphName == null)
            throw new IllegalStateException("Model header is only available for FullModels or DifferenceModels. Use isFullModel() or isDifferenceModel() to check.");

        return ModelHeader.wrap(getGraph(graphName));
    }

    default Graph getBody() {
        if(!this.isFullModel())
            throw new IllegalStateException("Body graph is only available for FullModels. Use isFullModel() to check.");
        return getDefaultGraph();
    }
}
