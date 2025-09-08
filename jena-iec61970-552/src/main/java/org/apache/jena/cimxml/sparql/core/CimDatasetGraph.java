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

import org.apache.jena.cimxml.CimHeaderVocabulary;
import org.apache.jena.cimxml.graph.CimModelHeader;
import org.apache.jena.cimxml.graph.DisjointMultiUnion;
import org.apache.jena.cimxml.graph.FastDeltaGraph;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.DatasetGraph;

import java.util.ArrayList;

/**
 * A {@link DatasetGraph} for CIM models.
 * <p>
 * This dataset can hold either a FullModel or a DifferenceModel.
 * <ul>
 *     <li>A FullModel contains a Model header graph and a body graph as the default graph.</li>
 *     <li>A DifferenceModel contains a Model header graph, a forward differences graph,
 *     a reverse differences graph and an optional preconditions graph as named graphs.</li>
 * </ul>
 * <p>
 * The model type can be determined using {@link #isFullModel()} and {@link #isDifferenceModel()}.
 * <p>
 * For FullModels the body graph can be accessed using {@link #getBody()} and the model header
 * using {@link #getModelHeader()}. To get a single graph containing both the model header and
 * the body graph one can use {@link #fullModelToSingleGraph()}.
 * <p>
 * For DifferenceModels the forward differences, reverse differences and preconditions graphs
 * can be accessed using {@link #getForwardDifferences()}, {@link #getReverseDifferences()}
 * and {@link #getPreconditions()}. The model header can be accessed using {@link #getModelHeader()}.
 * To convert a DifferenceModel to a FullModel one can use {@link #differenceModelToFullModel(CimDatasetGraph)}
 * by providing the predecessor FullModel as an argument. The resulting FullModel does not contain
 * the model header - only the body graph.
 */
public interface CimDatasetGraph extends DatasetGraph {

    /**
     * Checks if this dataset contains a FullModel.
     * @return true if this dataset contains a FullModel, false otherwise
     */
    default boolean isFullModel() {
        return containsGraph(CimHeaderVocabulary.TYPE_FULL_MODEL);
    }

    /**
     * Checks if this dataset contains a DifferenceModel.
     * @return true if this dataset contains a DifferenceModel, false otherwise
     */
    default boolean isDifferenceModel() {
        return containsGraph(CimHeaderVocabulary.TYPE_DIFFERENCE_MODEL);
    }

    /**
     * Gets the forward differences graph of this DifferenceModel.
     * @return the forward differences graph
     * @throws IllegalStateException if this dataset is not a DifferenceModel
     */
    default Graph getForwardDifferences() {
        if(!this.isDifferenceModel())
            throw new IllegalStateException("Forward differences are only available for DifferenceModels. Use isDifferentModel() to check.");
        return getGraph(CimHeaderVocabulary.GRAPH_FORWARD_DIFFERENCES);
    }

    /**
     * Gets the reverse differences graph of this DifferenceModel.
     * @return the reverse differences graph
     * @throws IllegalStateException if this dataset is not a DifferenceModel
     */
    default Graph getReverseDifferences() {
        if(!this.isDifferenceModel())
            throw new IllegalStateException("Reverse differences are only available for DifferenceModels. Use isDifferentModel() to check.");
        return getGraph(CimHeaderVocabulary.GRAPH_REVERSE_DIFFERENCES);
    }

    /**
     * Gets the preconditions graph of this DifferenceModel.
     * @return the preconditions graph
     * @throws IllegalStateException if this dataset is not a DifferenceModel
     */
    default Graph getPreconditions() {
        if(!this.isDifferenceModel())
            throw new IllegalStateException("Preconditions are only available for DifferenceModels. Use isDifferentModel() to check.");
        return getGraph(CimHeaderVocabulary.GRAPH_PRECONDITIONS);
    }

    /**
     * Gets the model header of this FullModel or DifferenceModel.
     * @return the model header
     * @throws IllegalStateException if this dataset is not a FullModel or DifferenceModel
     */
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

    /**
     * Gets the body graph of this FullModel.
     * @return the body graph
     * @throws IllegalStateException if this dataset is not a FullModel
     */
    default Graph getBody() {
        if(!this.isFullModel())
            throw new IllegalStateException("Body graph is only available for FullModels. Use isFullModel() to check.");
        return getDefaultGraph();
    }

    /**
     * Converts this FullModel to a single graph by combining the model header and the body graph.
     * @return a new Graph representing the FullModel - containing both the Model header and the body graph
     * @throws IllegalStateException if this dataset is not a FullModel
     */
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
     * Converts this DifferenceModel to a FullModel by applying the forward and reverse differences
     * to the provided predecessor FullModel.
     * @param predecessorFullModel the predecessor FullModel to which the differences will be applied
     * @return a new Graph representing the resulting FullModel - containing only the body graph
     * @throws IllegalStateException if this dataset is not a DifferenceModel
     * @throws IllegalArgumentException if the provided predecessorFullModel is not a FullModel,
     *                                  if its Model is not in the current Model.Supersedes,
     *                                  or if it does not contain all required preconditions
     */
    default Graph differenceModelToFullModel(CimDatasetGraph predecessorFullModel) {
        if(!this.isDifferenceModel())
            throw new IllegalStateException("Conversion to full model is only available for DifferenceModels. Use isDifferenceModel() to check.");
        if(!predecessorFullModel.isFullModel())
            throw new IllegalArgumentException("The provided predecessorFullModel dataset must be a FullModel. Use isFullModel() to check.");

        if(this.getModelHeader().getSupersedes().contains(predecessorFullModel.getModelHeader().getModel()))
            throw new IllegalArgumentException("The provided predecessorFullModel dataset Model must be in current Model.Supersedes.");

        var predecessorBody = predecessorFullModel.getBody();

        var preconditions = getPreconditions();
        if(preconditions != null && !preconditions.isEmpty()) {
            var missingPreconditions = new ArrayList<Triple>();
            preconditions.find().forEachRemaining(t -> {
                if(!predecessorBody.contains(t))
                    missingPreconditions.add(t);
            });
            if(!missingPreconditions.isEmpty()) {
                throw new IllegalArgumentException("The provided predecessorFullModel dataset does not contain all required preconditions. Missing preconditions: " + missingPreconditions);
            }
        }

        var forwardDifferences = getForwardDifferences();
        var reverseDifferences = getReverseDifferences();

        var deltaGraph = new FastDeltaGraph(predecessorBody, forwardDifferences, reverseDifferences);
        deltaGraph.getPrefixMapping().setNsPrefixes(this.getModelHeader().getPrefixMapping());

        return deltaGraph;
    }
}
