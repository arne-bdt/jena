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

package org.apache.jena.mem.graph;

import org.apache.jena.mem.graph.helper.JMHDefaultOptions;
import org.apache.jena.mem.graph.helper.Releases;
import org.apache.jena.mem.graph.helper.ResultSetSerialization;
import org.apache.jena.mem2.GraphMem2Fast;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetRewindable;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.io.File;
import java.util.List;

@State(Scope.Benchmark)
public class TestResultSerialization {

    @Param({
//            "cheeses-0.1.ttl",
//            "pizza.owl.rdf",
//            "xxx_CGMES_EQ.xml",
//            "xxx_CGMES_SSH.xml",
            "xxx_CGMES_TP.xml",
//            "RealGrid_EQ.xml",
//            "RealGrid_SSH.xml",
//            "RealGrid_TP.xml",
//            "RealGrid_SV.xml",
//            "bsbm-1m.nt.gz",
//            "bsbm-5m.nt.gz",
//            "bsbm-25m.nt.gz",
    })
    public String param0_GraphUri;

    private static String getFilePath(String fileName) {
        switch (fileName) {
            case "cheeses-0.1.ttl":
                return "../testing/cheeses-0.1.ttl";
            case "pizza.owl.rdf":
                return "../testing/pizza.owl.rdf";
            case "xxx_CGMES_EQ.xml":
                return "C:/temp/res_test/xxx_CGMES_EQ.xml";
            case "xxx_CGMES_SSH.xml":
                return "C:/temp/res_test/xxx_CGMES_SSH.xml";
            case "xxx_CGMES_TP.xml":
                return "C:/temp/res_test/xxx_CGMES_TP.xml";
            case "RealGrid_EQ.xml":
                return "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml";
            case "RealGrid_SSH.xml":
                return "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SSH.xml";
            case "RealGrid_TP.xml":
                return "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_TP.xml";
            case "RealGrid_SV.xml":
                return "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml";
            case "bsbm-1m.nt.gz":
                return "../testing/BSBM/bsbm-1m.nt.gz";
            case "bsbm-5m.nt.gz":
                return "../testing/BSBM/bsbm-5m.nt.gz";
            case "bsbm-25m.nt.gz":
                return "../testing/BSBM/bsbm-25m.nt.gz";
            default:
                throw new IllegalArgumentException("Unknown fileName: " + fileName);
        }
    }

    @Param({
//            "RS_XML",
//            "RS_JSON",
//            "RS_CSV",
//            "RS_TSV",
//            "RS_Text",
//            "RS_Thrift",
//            "RS_Thrift2",
            "RS_Protobuf",
            "RS_Protobuf2"
    })
    public String param1_RDFFormat;
    @Param({
            ResultSetSerialization.NO_COMPRESSOR,
            ResultSetSerialization.LZ4_FASTEST,
            ResultSetSerialization.GZIP
    })
    public String param2_Compressor;


    private ResultSetRewindable resultSetToSerialize;

    private ResultSetSerialization.SerializedData serializedResultSet;
    private Lang resultSetLang;



    private static Lang getResultSetLang(String rdfFormat) {
        switch (rdfFormat) {
            case "RS_XML":
                return ResultSetLang.RS_XML;
            case "RS_JSON":
                return ResultSetLang.RS_JSON;
            case "RS_CSV":
                return ResultSetLang.RS_CSV;
            case "RS_TSV":
                return ResultSetLang.RS_TSV;
            case "RS_Text":
                return ResultSetLang.RS_Text;
            case "RS_Thrift":
                return ResultSetLang.RS_Thrift;
            case "RS_Thrift2":
                return ResultSetLang.RS_Thrift2;
            case "RS_Protobuf":
                return ResultSetLang.RS_Protobuf;
            case "RS_Protobuf2":
                return ResultSetLang.RS_Protobuf2;
            default:
                throw new IllegalArgumentException("Unknown ResultSetLang: " + rdfFormat);
        }
    }

    @Test
    @Ignore
    public void loadSerializeAndDeserialize() {
        for(var file : List.of(
//                "../testing/cheeses-0.1.ttl",
//                "../testing/pizza.owl.rdf",
//                "C:/temp/res_test/xxx_CGMES_EQ.xml",
//                "C:/temp/res_test/xxx_CGMES_SSH.xml",
//                "C:/temp/res_test/xxx_CGMES_TP.xml",
//                "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml",
//                "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SSH.xml",
//                "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_TP.xml",
//                "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml",
                "../testing/BSBM/bsbm-1m.nt.gz"
            )) {
            final var g = new GraphMem2Fast();
            final var fileName = new File(file).getName();
            RDFDataMgr.read(g, file);
            final var resultSet = QueryExecutionFactory.create("SELECT * WHERE { ?s ?p ?o }", ModelFactory.createModelForGraph(g))
                    .execSelect().materialise().rewindable();
            for (var resultSetLang : List.of(
                    ResultSetLang.RS_XML,
                    ResultSetLang.RS_JSON,
                    ResultSetLang.RS_CSV,
                    ResultSetLang.RS_TSV,
//                    ResultSetLang.RS_Text,
                    ResultSetLang.RS_Thrift,
                    ResultSetLang.RS_Thrift2,
                    ResultSetLang.RS_Protobuf,
                    ResultSetLang.RS_Protobuf2
                )) {
                for (var compressor : List.of(ResultSetSerialization.NO_COMPRESSOR, ResultSetSerialization.LZ4_FASTEST, ResultSetSerialization.GZIP)) {
                    final var compressedGraph = ResultSetSerialization.serialize(resultSet, resultSetLang, compressor);
                    //print: "Size of output stream in format %resultSetLang% and with compressor %compressor% is xxx.xx MB.
                    System.out.printf("Size of %-20s in ResultSetLang %-30s and with compressor %-12s is %7.2f MB.  ResultSet::size: %8d\n", fileName, resultSetLang.toString(), compressor, compressedGraph.bytes().length / 1024.0 / 1024.0, resultSet.size());
                    resultSet.reset();
                }
            }
        }
    }

    @Benchmark
    public ResultSetSerialization.SerializedData serialize() {
        var result =  ResultSetSerialization.serialize(resultSetToSerialize, resultSetLang, param2_Compressor);
        resultSetToSerialize.reset();
        return result;
    }

    @Benchmark
    public ResultSet deserialize() {
        return ResultSetSerialization.deserialize(serializedResultSet);
    }

//    @Benchmark
    public ResultSet serializeAndDeserialize() {
        return ResultSetSerialization.deserialize(ResultSetSerialization.serialize(resultSetToSerialize, resultSetLang, param2_Compressor));
    }

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        var g = new GraphMem2Fast();
        Releases.current.readTriples(getFilePath(param0_GraphUri)).forEach(g::add);
        this.resultSetToSerialize = QueryExecutionFactory.create("SELECT * WHERE { ?s ?p ?o }", ModelFactory.createModelForGraph(g))
                .execSelect().materialise().rewindable();

        this.resultSetLang = getResultSetLang(param1_RDFFormat);

        this.serializedResultSet = ResultSetSerialization.serialize(resultSetToSerialize, resultSetLang, param2_Compressor);
        System.out.printf("\nSize of output stream in ResultSetLang %s is %4.2f MB. ResultSet::size: %8d.\n", resultSetLang.toString(), serializedResultSet.bytes().length / 1024.0 / 1024.0, resultSetToSerialize.size());
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JMHDefaultOptions.getDefaults(this.getClass())
                .warmupIterations(8)
                .measurementIterations(20)
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }
}
