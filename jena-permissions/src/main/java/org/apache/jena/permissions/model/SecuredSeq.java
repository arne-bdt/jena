/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jena.permissions.model;

import org.apache.jena.rdf.model.Seq;

/**
 * The interface for secured Seq instances.
 *
 * Use the SecuredSeq.Factory to create instances
 *
 * Sequence may have breaks in the order.
 * http://www.w3.org/TR/2004/REC-rdf-mt-20040210/#Containers
 * @deprecated To be removed.
 */
@Deprecated(forRemoval = true)
public interface SecuredSeq extends Seq, SecuredContainer {

}
