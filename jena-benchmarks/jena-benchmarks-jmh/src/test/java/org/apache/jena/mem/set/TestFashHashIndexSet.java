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
package org.apache.jena.mem.set;

import org.apache.jena.mem2.collection.specialized.FastHashIndexSet;
import org.junit.Test;

public class TestFashHashIndexSet {

    @Test
    public void testAdd() {
        var set = new FastHashIndexSet();
        assert set.tryAdd(0);
        assert set.tryAdd(1);
        assert set.tryAdd(2);

        assert set.contains(0);
        assert set.contains(1);
        assert set.contains(2);
        assert !set.isEmpty();
        assert set.size() == 3;

        assert !set.contains(3);

        assert set.tryRemove(0);
        assert !set.contains(0);
        assert set.size() == 2;

        assert set.tryRemove(2);
        assert !set.contains(2);
        assert set.size() == 1;

        assert !set.tryAdd(1);

        assert set.tryAdd(3);
        assert set.contains(3);
        assert set.size() == 2;

        assert set.tryRemove(3);
        assert set.tryRemove(1);
        assert set.isEmpty();
        assert set.size() == 0;
    }


}
