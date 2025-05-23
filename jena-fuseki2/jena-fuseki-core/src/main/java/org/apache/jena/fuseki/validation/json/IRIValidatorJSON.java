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

package org.apache.jena.fuseki.validation.json;

import static org.apache.jena.fuseki.validation.json.ValidatorJsonLib.getArgs;
import static org.apache.jena.fuseki.validation.json.ValidatorJsonLib.jErrors;
import static org.apache.jena.fuseki.validation.json.ValidatorJsonLib.jWarnings;

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.atlas.json.JsonBuilder;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.fuseki.servlets.ServletOps;
import org.apache.jena.iri3986.provider.IRIProvider3986;
import org.apache.jena.irix.IRIException;
import org.apache.jena.irix.IRIProvider;
import org.apache.jena.irix.IRIx;

public class IRIValidatorJSON {

    public IRIValidatorJSON() { }

    static final String paramIRI           = "iri";

    // Output is an object  { "iris" : [ ] }
    // with array entries   { "iri": "" , "error": [], "warnings": [] }
    static final String jIRIs    = "iris";
    static final String jIRI     = "iri";

    public static JsonObject execute(ValidationAction action) {
        JsonBuilder obj = new JsonBuilder();
        obj.startObject();

        String args[] = getArgs(action, paramIRI);
        if ( args == null || args.length == 0 ) {
            ServletOps.errorBadRequest("No IRIs supplied");
            return null;
        }

        obj.key(jIRIs);
        obj.startArray();

        IRIProvider provider = new IRIProvider3986();

        for ( String iriStr : args ) {
            obj.startObject();
            obj.key(jIRI).value(iriStr);
            List<String> errors = new ArrayList<>();
            List<String> warnings = new ArrayList<>();
            try {
                IRIx iri = provider.create(iriStr);
                System.out.println(iriStr + " ==> " + iri);
                if ( iri.isRelative() )
                    if ( iri.isRelative() )
                        warnings.add("Relative IRI: " + iriStr);
                iri.handleViolations((error,msg)->{
                    if ( error )
                        errors.add(msg);
                    else
                        warnings.add(msg);
                });
            } catch (IRIException ex) {
                errors.add("Bad IRI: "+ex.getMessage());
            }
            obj.key(jErrors);
            obj.startArray();
            for ( String msg : errors )
                obj.value(msg);
            obj.finishArray();

            obj.key(jWarnings);
            obj.startArray();
            for ( String msg : warnings )
                obj.value(msg);
            obj.finishArray();

            obj.finishObject();
        }
        obj.finishArray();
        obj.finishObject();
        return obj.build().getAsObject();
    }
}
