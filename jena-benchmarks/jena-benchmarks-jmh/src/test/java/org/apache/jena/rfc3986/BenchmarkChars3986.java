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

package org.apache.jena.rfc3986;

import org.apache.jena.JMHDefaultOptions;
import org.apache.jena.sys.JenaSystem;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@State(Scope.Benchmark)
public class BenchmarkChars3986 {

    static {
        JenaSystem.init();
        org.apache.shadedJena550.sys.JenaSystem.init();
    }

    @Param({"current", "5.5.0"})
    public String param0_jenaVersion;

    private Function<Character, Boolean> isAlpha;
    private Function<Character, Boolean> isAlphaNum;
    private Function<Character, Boolean> isDigit;
    private Function<Character, Boolean> isHexDigit;
    private List<Character> chars;

    @Setup
    public void setupTrial() {
        var iterations = 100000; // Number of times to repeat the character set
        chars = new ArrayList<>(iterations*256);
        for(int iteration = 0; iteration < iterations; iteration++) {
            for (int i = 0; i < 256; i++) {
                chars.add((char) i);
            }
        }
        switch (param0_jenaVersion) {
            case "current":
                isAlpha = Chars3986::isAlpha;
                isAlphaNum = Chars3986::isAlphaNum;
                isDigit = Chars3986::isDigit;
                isHexDigit = Chars3986::isHexDigit;
                break;
            case "5.5.0":
                isAlpha = org.apache.shadedJena550.rfc3986.Chars3986::isAlpha;
                isAlphaNum = org.apache.shadedJena550.rfc3986.Chars3986::isAlphaNum;
                isDigit = org.apache.shadedJena550.rfc3986.Chars3986::isDigit;
                isHexDigit = org.apache.shadedJena550.rfc3986.Chars3986::isHexDigit;
                break;
            default:
                throw new IllegalArgumentException("Unknown jena version: " + param0_jenaVersion);
        }
    }

    @Benchmark
    public boolean benchIsAlpha() {
        boolean r = true;
        for (char c : chars) {
            r &= isAlpha.apply(c);
        }
        return r;
    }

    @Benchmark
    public boolean benchIsAlphaNum() {
        boolean r = true;
        for (char c : chars) {
            r &= isAlphaNum.apply(c);
        }
        return r;
    }

    @Benchmark
    public boolean benchIsDigit() {
        boolean r = true;
        for (char c : chars) {
            r &= isDigit.apply(c);
        }
        return r;
    }

    @Benchmark
    public boolean benchIsHexDigit() {
        boolean r = true;
        for (char c : chars) {
            r &= isHexDigit.apply(c);
        }
        return r;
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JMHDefaultOptions.getDefaults(this.getClass()).build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }
}
