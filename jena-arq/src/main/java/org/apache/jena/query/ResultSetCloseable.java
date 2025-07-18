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

package org.apache.jena.query;

import org.apache.jena.riot.resultset.ResultSetOnClose;

/**
 * A {@link ResultSet} that closes the associated {@link QueryExecution}
 * via {@link AutoCloseable}.
 */
public class ResultSetCloseable extends ResultSetOnClose implements AutoCloseable {

    /** Return a closable result set for a {@link QueryExecution}.
     * The {@link QueryExecution} must be for a {@code SELECT} query.
     * @param queryExecution {@code QueryExecution} must be for a {@code SELECT} query.
     * @return ResultSetCloseable
     */
    public static ResultSetCloseable closeableResultSet(QueryExecution queryExecution) {
        if ( queryExecution.getQuery() != null && ! queryExecution.getQuery().isSelectType() )
            throw new QueryException("Not an execution for a SELECT query");
        return new ResultSetCloseable(queryExecution.execSelect(), queryExecution) ;
    }

    /** @deprecated The constructor will become private. Use {@link #closeableResultSet}. */
    @Deprecated
    public ResultSetCloseable(ResultSet rs, QueryExecution qexec) {
        super(rs, ()->qexec.close()) ;
    }
}
