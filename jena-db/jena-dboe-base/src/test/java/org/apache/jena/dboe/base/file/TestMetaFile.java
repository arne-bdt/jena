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

package org.apache.jena.dboe.base.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.jena.dboe.ConfigTestDBOE;
import org.apache.jena.dboe.sys.Names;

public class TestMetaFile
{
    String testfile = null;
    String testfileMeta = null;

    @Before public void before() {
        testfile = ConfigTestDBOE.getTestingDir()+"/file";
        testfileMeta = ConfigTestDBOE.getTestingDir()+"/file."+Names.extMeta;
        File f = new File(testfileMeta);
        f.delete();
    }

    @Test public void meta1() {
        clear();
        MetaFile f = new MetaFile("META", testfile);
        assertFalse(new File(testfileMeta).exists());
        f.setProperty("key", "value");
        f.flush();
        assertTrue(new File(f.getFilename()).exists());
    }

    @Test public void meta2() {
        clear();
        MetaFile f = new MetaFile("META", testfile);
        f.setProperty("test.value1", "1");
        f.flush();
        MetaFile f2 = new MetaFile("META", testfile);
        assertEquals("1", f2.getProperty("test.value1"));
        assertNull(f2.getProperty("test.value.other"));
    }

    // Test MetaBase

    @After public void afterClass()
    { clear(); }

    private void clear() {
        File f = new File(testfileMeta);
        f.delete();
    }
}
