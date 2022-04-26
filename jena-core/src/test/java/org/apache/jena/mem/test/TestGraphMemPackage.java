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

package org.apache.jena.mem.test;


import junit.framework.*;
import org.apache.jena.mem.GraphMem;
import org.apache.jena.mem.GraphMemUsingHashMap;

/**
 * @deprecated
 * This test is only needed for the deprecated {@link GraphMem}, which is replaced by {@link GraphMemUsingHashMap}
 * {@link TestGraphMemUsingHashMapPackage} is the replacement for this test.
 */
@Deprecated(since = "4.5.0")
public class TestGraphMemPackage extends TestCase
    {
    public static TestSuite suite()
        { 
        TestSuite result = new TestSuite();
        result.addTest( TestGraphMem.suite() );
        result.addTest( TestGraphMem2.suite() );
        return result;
        }
    }
