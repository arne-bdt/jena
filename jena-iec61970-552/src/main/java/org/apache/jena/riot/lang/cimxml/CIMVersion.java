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
package org.apache.jena.riot.lang.cimxml;

import java.util.Objects;

public enum CIMVersion {
    /** No CIM version specified */
    NO_CIM,
    /**
     * CIM version 16.
     * This version is used in CGMES v2.4.15
     */
    CIM_16,
    /**
     * CIM version 17.
     * This version is used in CGMES v3.0
     * */
    CIM_17,
    /**
     * CIM version 18.
     * There is no matching version of CGMES yet.
     */
    CIM_18;

    public static CIMVersion fromCimNamespacePrefixUri(String prefixURI) {
        Objects.requireNonNull(prefixURI);
        return switch(prefixURI) {
            case "http://iec.ch/TC57/2013/CIM-schema-cim16#" -> CIMVersion.CIM_16;
            case "http://iec.ch/TC57/CIM100#" -> CIMVersion.CIM_17;
            case "https://cim.ucaiug.io/ns#" -> CIMVersion.CIM_18;
            default -> CIMVersion.NO_CIM;
        };
    }
}
