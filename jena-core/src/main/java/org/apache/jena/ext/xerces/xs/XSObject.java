/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.ext.xerces.xs;

/**
 * The <code>XSObject</code> is a base object for the XML Schema component 
 * model. 
 */
public interface XSObject {

    /**
     * The name of type <code>NCName</code>, as defined in XML Namespaces, of 
     * this declaration specified in the <code>{name}</code> property of the 
     * component or <code>null</code> if the definition of this component 
     * does not have a <code>{name}</code> property. For anonymous types, 
     * the processor must construct and expose an anonymous type name that 
     * is distinct from the name of every named type and the name of every 
     * other anonymous type.
     */
    public String getName();

    /**
     *  The [target namespace] of this object, or <code>null</code> if it is 
     * unspecified. 
     */
    public String getNamespace();

}
