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

package org.apache.jena.cimxml.sparql.core;

import org.apache.jena.cimxml.graph.CIMGraph;
import org.apache.jena.cimxml.graph.DisjointMultiUnion;
import org.apache.jena.cimxml.graph.FastDeltaGraph;
import org.apache.jena.graph.*;
import org.apache.jena.cimxml.CimHeaderVocabulary;
import org.apache.jena.cimxml.graph.CimModelHeader;
import org.apache.jena.sparql.core.DatasetGraph;

public interface CimDatasetGraph extends DatasetGraph {

    default boolean isFullModel() {
        return containsGraph(CimHeaderVocabulary.TYPE_FULL_MODEL);
    }

    default boolean isDifferenceModel() {
        return containsGraph(CimHeaderVocabulary.TYPE_DIFFERENCE_MODEL);
    }

    default Graph getForwardDifferences() {
        if(!this.isDifferenceModel())
            throw new IllegalStateException("Forward differences are only available for DifferenceModels. Use isDifferentModel() to check.");
        return getGraph(CimHeaderVocabulary.GRAPH_FORWARD_DIFFERENCES);
    }

    default Graph getReverseDifferences() {
        if(!this.isDifferenceModel())
            throw new IllegalStateException("Reverse differences are only available for DifferenceModels. Use isDifferentModel() to check.");
        return getGraph(CimHeaderVocabulary.GRAPH_REVERSE_DIFFERENCES);
    }

    default Graph getPreconditions() {
        if(!this.isDifferenceModel())
            throw new IllegalStateException("Preconditions are only available for DifferenceModels. Use isDifferentModel() to check.");
        return getGraph(CimHeaderVocabulary.GRAPH_PRECONDITIONS);
    }

    default CimModelHeader getModelHeader() {
        var graphName = isFullModel()
                ? CimHeaderVocabulary.TYPE_FULL_MODEL
                : isDifferenceModel()
                    ? CimHeaderVocabulary.TYPE_DIFFERENCE_MODEL
                    : null;

        if(graphName == null)
            throw new IllegalStateException("Model header is only available for FullModels or DifferenceModels. Use isFullModel() or isDifferenceModel() to check.");

        return CimModelHeader.wrap(getGraph(graphName));
    }

    default Graph getBody() {
        if(!this.isFullModel())
            throw new IllegalStateException("Body graph is only available for FullModels. Use isFullModel() to check.");
        return getDefaultGraph();
    }

    default Graph fullModelToSingleGraph() {
        if(!this.isFullModel())
            throw new IllegalStateException("Full model graph is only available for FullModels. Use isFullModel() to check.");

        var header = getModelHeader();
        var body = getBody();

        var union = new DisjointMultiUnion(header, body);
        union.getPrefixMapping().setNsPrefixes(header.getPrefixMapping());

        return union;
    }

    /**
     * Converts this DifferenceModel to a FullModel by applying the forward and reverse differences to the provided predecessor FullModel.
     * The model header of the resulting FullModel is not included in the returned Graph.
     * @param predecessorFullModel the predecessor FullModel to apply the differences to
     * @return a new Graph representing the resulting FullModel - without the Model header
     * @throws IllegalStateException if this dataset is not a DifferenceModel
     * @throws IllegalArgumentException if the provided predecessorFullModel is not a FullModel or if its Model is not in the current Model.Supersedes
     */
    default Graph differenceModelToFullModel(CimDatasetGraph predecessorFullModel) {
        if(!this.isDifferenceModel())
            throw new IllegalStateException("Conversion to full model is only available for DifferenceModels. Use isDifferenceModel() to check.");
        if(!predecessorFullModel.isFullModel())
            throw new IllegalArgumentException("The provided predecessorFullModel dataset must be a FullModel. Use isFullModel() to check.");

        if(this.getModelHeader().getSupersedes().contains(predecessorFullModel.getModelHeader().getModel()))
            throw new IllegalArgumentException("The provided predecessorFullModel dataset Model must be in current Model.Supersedes.");

        var predecessorBody = predecessorFullModel.getBody();
        var forwardDifferences = getForwardDifferences();
        var reverseDifferences = getReverseDifferences();

        var deltaGraph = new FastDeltaGraph(predecessorBody, forwardDifferences, reverseDifferences);
        deltaGraph.getPrefixMapping().setNsPrefixes(this.getModelHeader().getPrefixMapping());

        return deltaGraph;
    }
}
