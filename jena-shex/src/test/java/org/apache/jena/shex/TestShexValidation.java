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

package org.apache.jena.shex;

import org.apache.jena.arq.junit.manifest.Manifests;
import org.apache.jena.arq.junit.runners.Label;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.riot.SysRIOT;
import org.apache.jena.shex.runner.RunnerShexValidation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

@RunWith(RunnerShexValidation.class)
@Label("Shex Validation")
@Manifests({
    "src/test/files/spec/validation/manifest.ttl"
})

// Syntax are files in:
// TestShexSyntax:    "src/test/files/spec/syntax"
// TestShexBadSyntax: "src/test/files/spec/negativeSyntax"

public class TestShexValidation {
    private static boolean bVerboseWarnings;
    private static boolean bWarnOnUnknownFunction;
    private static String logLevel;
    @BeforeClass
    public static void beforeClass() {
        logLevel = LogCtl.getLevel(SysRIOT.getLogger());
        LogCtl.setLevel(SysRIOT.getLogger(), "ERROR");
    }

    @AfterClass
    public static void afterClass() {
        LogCtl.setLevel(SysRIOT.getLogger(), logLevel);
    }
}
