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

package org.apache.jena.fuseki.mgt;

import static org.apache.jena.riot.WebContent.charsetUTF8;
import static org.apache.jena.riot.WebContent.contentTypeTextPlain;

import java.io.IOException;

import jakarta.servlet.ServletOutputStream;
import org.apache.jena.fuseki.ctl.ActionCtl;
import org.apache.jena.fuseki.servlets.HttpAction;
import org.apache.jena.fuseki.servlets.ServletOps;

public class ActionLogs extends ActionCtl
{
    public ActionLogs() { super(); }

    @Override
    public void validate(HttpAction action) {}

    @Override
    public void execGet(HttpAction action) {
        executeLifecycle(action);
    }

    @Override
    public void execute(HttpAction action) {
        try {
            ServletOutputStream out = action.getResponseOutputStream();
            action.setResponseContentType(contentTypeTextPlain);
            action.setResponseCharacterEncoding(charsetUTF8);
            out.println("Not implemented yet");
            out.println();
            out.flush();
            ServletOps.success(action);
        } catch (IOException ex) { ServletOps.errorOccurred(ex); }
    }
}
