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

package org.apache.jena.mem.jmh;

import org.apache.jena.graph.Triple;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

@State(Scope.Benchmark)
public class TestGraphStreamAll extends AbstractTestGraphBaseWithFilledGraph {

    @Benchmark
    public List<Triple> graphStream() {
        var found = sut.stream().collect(Collectors.toList());
        assertEquals(sut.size(), found.size());
        return found;
    }

    @Benchmark
    public List<Triple> graphStreamParallel() {
        if(triples.size() < 10000) { /*to avoid waiting for blocking parallel execution*/
            var found = sut.stream().collect(Collectors.toList());
            assertEquals(sut.size(), found.size());
            return found;
        } else {
            var found = sut.stream().parallel().collect(Collectors.toList());
            assertEquals(sut.size(), found.size());
            return found;
        }
    }

    @Test
    public void benchmark() throws Exception {
        var opt = setupOptionsBuilderWithDefaultOptions(this.getClass())
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }
}