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

package org.apache.jena.reasoner.transitiveReasoner;

import org.apache.jena.graph.* ;
import org.apache.jena.rdf.model.* ;
import org.apache.jena.reasoner.* ;
import org.apache.jena.vocabulary.RDFS ;
import org.apache.jena.vocabulary.ReasonerVocabulary ;

/**
 * A  simple "reasoner" used to help with API development.
 * <p>This reasoner caches a transitive closure of the subClass and
 * subProperty graphs. The generated infGraph allows both the direct
 * and closed versions of these properties to be retrieved. The cache is
 * built when the tbox is bound in but if the final data graph
 * contains additional subProperty/subClass declarations then the
 * cache has to be rebuilt.</p>
 * <p>
 * The triples in the tbox (if present) will also be included
 * in any query. Any of tbox or data graph are allowed to be null.</p>
 */
public class TransitiveReasoner implements Reasoner {

    /** The precomputed cache of the subClass graph */
    protected TransitiveGraphCache subClassCache;

    /** The precomputed cache of the subProperty graph */
    protected TransitiveGraphCache subPropertyCache;

    /** The graph registered as the schema, if any */
    protected Finder tbox = null;

    /** The direct (minimal) version of the subPropertyOf property */
    public static final Node directSubPropertyOf =
        ReasonerRegistry.makeDirect(RDFS.Nodes.subPropertyOf);

    /** The direct (minimal) version of the subClassOf property */
    public static final Node directSubClassOf =
        ReasonerRegistry.makeDirect(RDFS.Nodes.subClassOf);

    /** The normal subPropertyOf property */
    public static final Node subPropertyOf = RDFS.Nodes.subPropertyOf;

    /** The normal subClassOf property */
    public static final Node subClassOf = RDFS.Nodes.subClassOf;

    /** Constructor */
    public TransitiveReasoner() {
        subClassCache = new TransitiveGraphCache(directSubClassOf, subClassOf);
        subPropertyCache = new TransitiveGraphCache(directSubPropertyOf, subPropertyOf);
    }

    /**
     * Private constructor used by bindSchema when
     * returning a partially bound reasoner instance.
     */
    protected TransitiveReasoner(Finder tbox,
                    TransitiveGraphCache subClassCache,
                    TransitiveGraphCache subPropertyCache) {
        this.tbox = tbox;
        this.subClassCache = subClassCache;
        this.subPropertyCache = subPropertyCache;
    }

    /**
     * Return a description of the capabilities of this reasoner encoded in
     * RDF. These capabilities may be static or may depend on configuration
     * information supplied at construction time. May be null if there are
     * no useful capabilities registered.
     */
    @Override
    public Model getReasonerCapabilities() {
        return TransitiveReasonerFactory.theInstance().getCapabilities();
    }

    /**
     * Add a configuration description for this reasoner into a partial
     * configuration specification model.
     * @param configSpec a Model into which the configuration information should be placed
     * @param base the Resource to which the configuration parameters should be added.
     */
    @Override
    public void addDescription(Model configSpec, Resource base) {
        // No configuration
    }

    /**
     * Determine whether the given property is recognized and treated specially
     * by this reasoner. This is a convenience packaging of a special case of getCapabilities.
     * @param property the property which we want to ask the reasoner about, given as a Node since
     * this is part of the SPI rather than API
     * @return true if the given property is handled specially by the reasoner.
     */
    @Override
    public boolean supportsProperty(Property property) {
        ReasonerFactory rf = TransitiveReasonerFactory.theInstance();
        Model caps = rf.getCapabilities();
        Resource root = caps.getResource(rf.getURI());
        return caps.contains(root, ReasonerVocabulary.supportsP, property);
    }

    /**
     * Extracts all of the subClass and subProperty declarations from
     * the given schema/tbox and caches the resultant graphs.
     * It can only be used once, can't stack up multiple tboxes this way.
     * This limitation could be lifted - the only difficulty is the need to
     * reprocess all the earlier tboxes if a new subPropertyOf subPropertyOf
     * subClassOf is discovered.
     * @param tbox schema containing the property and class declarations
     */
    @Override
    public Reasoner bindSchema(Graph tbox) throws ReasonerException {
        return bindSchema(new FGraph(tbox));
    }

    /**
     * Extracts all of the subClass and subProperty declarations from
     * the given schema/tbox and caches the resultant graphs.
     * It can only be used once, can't stack up multiple tboxes this way.
     * This limitation could be lifted - the only difficulty is the need to
     * reprocess all the earlier tboxes if a new subPropertyOf subPropertyOf
     * subClassOf is discovered.
     * @param tbox schema containing the property and class declarations
     */
    @Override
    public Reasoner bindSchema(Model tbox) throws ReasonerException {
        return bindSchema(new FGraph(tbox.getGraph()));
    }


    /**
     * Extracts all of the subClass and subProperty declarations from
     * the given schema/tbox and caches the resultant graphs.
     * It can only be used once, can't stack up multiple tboxes this way.
     * This limitation could be lifted - the only difficulty is the need to
     * reprocess all the earlier tboxes if a new subPropertyOf subPropertyOf
     * subClassOf is discovered.
     * @param tbox schema containing the property and class declarations
     */
    Reasoner bindSchema(Finder tbox) throws ReasonerException {
        if (this.tbox != null) {
            throw new ReasonerException("Attempt to bind multiple rulesets - disallowed for now");
        }
        TransitiveGraphCache sCc = new TransitiveGraphCache(directSubClassOf, subClassOf);
        TransitiveGraphCache sPc = new TransitiveGraphCache(directSubPropertyOf, subPropertyOf);
        TransitiveEngine.cacheSubPropUtility(tbox, sPc);
        TransitiveEngine.cacheSubClassUtility(tbox, sPc, sCc);

        return new TransitiveReasoner(tbox, sCc, sPc);
    }

    /**
     * Attach the reasoner to a set of RDF ddata to process.
     * The reasoner may already have been bound to specific rules or ontology
     * axioms (encoded in RDF) through earlier bindRuleset calls.
     * @param data the RDF data to be processed, some reasoners may restrict
     * the range of RDF which is legal here (e.g. syntactic restrictions in OWL).
     * @return an inference graph through which the data+reasoner can be queried.
     * @throws ReasonerException if the data is ill-formed according to the
     * constraints imposed by this reasoner.
     */
    @Override
    public InfGraph bind(Graph data) throws ReasonerException {
        return new TransitiveInfGraph(data, this);
    }

    /**
     * Switch on/off drivation logging.
     * If set to true then the InfGraph created from the bind operation will start
     * life with recording of derivations switched on. This is currently only of relevance
     * to rule-based reasoners.
     * <p>
     * Default - false.
     */
    @Override
    public void setDerivationLogging(boolean logOn) {
        // Irrelevant to this reasoner
    }

    /**
      * Set a configuration paramter for the reasoner. In the case of the this
      * reasoner there are no configuration parameters and this method is simply
      * here to meet the interfaces specification
      *
      * @param parameter the property identifying the parameter to be changed
      * @param value the new value for the parameter, typically this is a wrapped
      * java object like Boolean or Integer.
      */
     @Override
    public void setParameter(Property parameter, Object value) {
         throw new IllegalParameterException(parameter.toString());
     }

    /**
     * Accessor used during infgraph construction - return the cached
     * version of the subProperty lattice.
     */
    public TransitiveGraphCache getSubPropertyCache() {
        return subPropertyCache;
    }

    /**
     * Accessor used during infgraph construction - return the cached
     * version of the subClass lattice.
     */
    public TransitiveGraphCache getSubClassCache() {
        return subClassCache;
    }

    /**
     * Accessor used during infgraph construction - return the partially
     * bound tbox, if any.
     */
    public Finder getTbox() {
        return tbox;
    }

    /**
     * Return the Jena Graph Capabilties that the inference graphs generated
     * by this reasoner are expected to conform to.
     */
    @Deprecated
    @Override
    public Capabilities getGraphCapabilities() {
        return BaseInfGraph.reasonerInfCapabilities;
    }

}
