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

package org.apache.jena.riot.lang;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.StringReader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.ErrorHandlerTestLib.ExFatal;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.system.RiotLib;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.riot.tokens.Tokenizer;
import org.apache.jena.riot.tokens.TokenizerText;

public class TestLangRdfJson {
    @SuppressWarnings("deprecation")
    @Test
    public void rdfjson_get_jena_reader() {
        Model m = ModelFactory.createDefaultModel();
        m.getReader("RDF/JSON");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void rdfjson_get_jena_writer() {
        Model m = ModelFactory.createDefaultModel();
        m.getWriter("RDF/JSON");
    }

    @Test
    public void rdfjson_read_empty_graph() {
        String s = "{}";
        String s2 = "";

        assertEquals(0, parseCount(s));

        Model m = parseToModelRdfJson(s);
        Model m2 = parseToModelNTriples(s2);
        assertTrue(m.isIsomorphicWith(m2));
    }

    public void rdfjson_valid_trailing_comment() {
        String s = "{}//Comment";

        assertEquals(0, parseCount(s));
    }

    @Test
    public void rdfjson_read_simple_uri_object() {
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\" : \"uri\" , \"value\" : \"http://example.org/object\" } ] } }";
        String s2 = "<http://example.org/subject> <http://example.org/predicate> <http://example.org/object> .";

        assertEquals(1, parseCount(s));

        Model m = parseToModelRdfJson(s);
        Model m2 = parseToModelNTriples(s2);
        assertTrue(m.isIsomorphicWith(m2));
    }

    @Test
    public void rdfjson_read_simple_bnode_object() {
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\" : \"bnode\" , \"value\" : \"_:id\" } ] } }";
        String s2 = "<http://example.org/subject> <http://example.org/predicate> _:id .";

        assertEquals(1, parseCount(s));

        Model m = parseToModelRdfJson(s);
        Model m2 = parseToModelNTriples(s2);
        assertTrue(m.isIsomorphicWith(m2));
    }

    @Test
    public void rdfjson_read_simple_bnode_subject() {
        String s = "{ \"_:id\" : { \"http://example.org/predicate\" : [ { \"type\" : \"uri\" , \"value\" : \"http://example.org/object\" } ] } }";
        String s2 = "_:id <http://example.org/predicate> <http://example.org/object> .";

        assertEquals(1, parseCount(s));

        Model m = parseToModelRdfJson(s);
        Model m2 = parseToModelNTriples(s2);
        assertTrue(m.isIsomorphicWith(m2));
    }

    @Test
    public void rdfjson_read_simple_plainliteral_object() {
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\" : \"literal\" , \"value\" : \"some text\" } ] } }";
        String s2 = "<http://example.org/subject> <http://example.org/predicate> \"some text\" .";

        assertEquals(1, parseCount(s));

        Model m = parseToModelRdfJson(s);
        Model m2 = parseToModelNTriples(s2);
        assertTrue(m.isIsomorphicWith(m2));
    }

    @Test
    public void rdfjson_read_simple_langliteral_object() {
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\" : \"literal\" , \"value\" : \"some text\", \"lang\" : \"en-gb\" } ] } }";
        String s2 = "<http://example.org/subject> <http://example.org/predicate> \"some text\"@en-gb .";

        assertEquals(1, parseCount(s));

        Model m = parseToModelRdfJson(s);
        Model m2 = parseToModelNTriples(s2);
        assertTrue(m.isIsomorphicWith(m2));
    }

    @Test
    public void rdfjson_read_simple_typedliteral_object() {
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\" : \"literal\" , \"value\" : \"some text\", \"datatype\" : \"http://example.org/datatype\" } ] } }";
        String s2 = "<http://example.org/subject> <http://example.org/predicate> \"some text\"^^<http://example.org/datatype> .";

        assertEquals(1, parseCount(s));

        Model m = parseToModelRdfJson(s);
        Model m2 = parseToModelNTriples(s2);
        assertTrue(m.isIsomorphicWith(m2));
    }

    @Test
    public void rdfjson_read_objectlist_uris() {
        // @formatter:off
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\" : \"uri\" , \"value\" : \"http://example.org/object\" } , { \"type\" : \"uri\" , \"value\" : \"http://example.org/object2\" } ] } }";
        String s2 = String.join("\n"
                               , "<http://example.org/subject> <http://example.org/predicate> <http://example.org/object> ."
                               , "<http://example.org/subject> <http://example.org/predicate> <http://example.org/object2> .");
        // @formatter:on

        assertEquals(2, parseCount(s));

        Model m = parseToModelRdfJson(s);
        Model m2 = parseToModelNTriples(s2);
        assertTrue(m.isIsomorphicWith(m2));
    }

    @Test
    public void rdfjson_read_objectlist_literals() {
        // @formatter:off
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\" : \"literal\" , \"value\" : \"some text\" } , { \"type\" : \"literal\" , \"value\" : \"more text\" } ] } }";
        String s2 = String.join("\n"
                               , "<http://example.org/subject> <http://example.org/predicate> \"some text\" ."
                               , "<http://example.org/subject> <http://example.org/predicate> \"more text\" .");
        // @formatter:on

        assertEquals(2, parseCount(s));

        Model m = parseToModelRdfJson(s);
        Model m2 = parseToModelNTriples(s2);
        assertTrue(m.isIsomorphicWith(m2));
    }

    @Test
    public void rdfjson_read_objectlist_literals2() {
        // @formatter:off
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\" : \"literal\" , \"value\" : \"some text\" } , { \"type\" : \"literal\" , \"value\" : \"more text\", \"lang\" : \"en-gb\" } ] } }";
        String s2 = String.join("\n"
                               , "<http://example.org/subject> <http://example.org/predicate> \"some text\" ."
                               , "<http://example.org/subject> <http://example.org/predicate> \"more text\"@en-gb .");
        // @formatter:on

        assertEquals(2, parseCount(s));

        Model m = parseToModelRdfJson(s);
        Model m2 = parseToModelNTriples(s2);
        assertTrue(m.isIsomorphicWith(m2));
    }

    @Test
    public void rdfjson_read_objectlist_literals3() {
        // @formatter:off
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\" : \"literal\" , \"value\" : \"some text\" } , { \"type\" : \"literal\" , \"value\" : \"more text\", \"datatype\" : \"http://example.org/datatype\" } ] } }";
        String s2 = String.join("\n"
                               , "<http://example.org/subject> <http://example.org/predicate> \"some text\" ."
                               , "<http://example.org/subject> <http://example.org/predicate> \"more text\"^^<http://example.org/datatype> .");
        // @formatter:on

        assertEquals(2, parseCount(s));

        Model m = parseToModelRdfJson(s);
        Model m2 = parseToModelNTriples(s2);
        assertTrue(m.isIsomorphicWith(m2));
    }

    @Test
    public void rdfjson_read_objectlist_bnodes() {
        // @formatter:off
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\" : \"bnode\" , \"value\" : \"_:one\" } , { \"type\" : \"bnode\" , \"value\" : \"_:two\" } ] } }";
        String s2 = String.join("\n"
                               ,"<http://example.org/subject> <http://example.org/predicate> _:a ."
                               , "<http://example.org/subject> <http://example.org/predicate> _:b .");
        // @formatter:on

        assertEquals(2, parseCount(s));

        Model m = parseToModelRdfJson(s);
        Model m2 = parseToModelNTriples(s2);
        assertTrue(m.isIsomorphicWith(m2));
    }

    @Test
    public void rdfjson_read_objectlist_mixed() {
        // @formatter:off
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\" : \"uri\" , \"value\" : \"http://example.org/object\" } , { \"type\" : \"literal\" , \"value\" : \"some text\" } , { \"type\" : \"bnode\" , \"value\" : \"_:id\" } ] } }";
        String s2 = String.join("\n"
                               , "<http://example.org/subject> <http://example.org/predicate> <http://example.org/object> ."
                               , "<http://example.org/subject> <http://example.org/predicate> \"some text\" ."
                               , "<http://example.org/subject> <http://example.org/predicate> _:id .");
        // @formatter:on

        assertEquals(3, parseCount(s));

        Model m = parseToModelRdfJson(s);
        Model m2 = parseToModelNTriples(s2);
        assertTrue(m.isIsomorphicWith(m2));
    }

    @Test
    public void rdfjson_read_predicatelist() {
        // @formatter:off
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\" : \"uri\" , \"value\" : \"http://example.org/object\" } ] , \"http://example.org/predicate2\" : [ { \"type\" : \"uri\" , \"value\" : \"http://example.org/object\" } ] } }";
        String s2 = String.join("\n"
                                , "<http://example.org/subject> <http://example.org/predicate> <http://example.org/object> ."
                                , "<http://example.org/subject> <http://example.org/predicate2> <http://example.org/object> .");
        // @formatter:on

        assertEquals(2, parseCount(s));

        Model m = parseToModelRdfJson(s);
        Model m2 = parseToModelNTriples(s2);
        assertTrue(m.isIsomorphicWith(m2));
    }

    @Test
    public void rdfjson_read_subjectlist() {
        // @formatter:off
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\" : \"uri\" , \"value\" : \"http://example.org/object\" } ] } , \"http://example.org/subject2\" : { \"http://example.org/predicate\" : [ { \"type\" : \"uri\" , \"value\" : \"http://example.org/object\" } ] } }";
        String s2 = String.join("\n"
                               ,"<http://example.org/subject> <http://example.org/predicate> <http://example.org/object> ."
                               , "<http://example.org/subject2> <http://example.org/predicate> <http://example.org/object> .");
        // @formatter:on

        assertEquals(2, parseCount(s));

        Model m = parseToModelRdfJson(s);
        Model m2 = parseToModelNTriples(s2);
        assertTrue(m.isIsomorphicWith(m2));
    }

    @Test
    public void rdfjson_read_complex() {
        // @formatter:off
        String s = String.join("\n"
                 , "{ \"http://example.org/subject\" :"
                 , "    {"
                 , "         \"http://example.org/predicate\" :"
                 , "        ["
                 , "            { \"type\" : \"uri\" , \"value\" : \"http://example.org/object\" }"
                 , "        ] ,"
                 , "        \"http://example.org/predicate2\" :"
                 , "        ["
                 , "            { \"type\" : \"literal\", \"value\" : \"some text\" }"
                 , "        ]"
                 , "    } ,"
                 , " \"http://example.org/subject2\" :"
                 , "    {"
                 , "        \"http://example.org/predicate\" :"
                 , "        ["
                 , "            { \"type\" : \"bnode\" , \"value\" : \"_:id\" } ,"
                 , "            { \"type\" : \"literal\" , \"value\" : \"more text\" , \"datatype\" : \"http://example.org/datatype\" }"
                 , "        ]"
                 , "    }"
                 , "}");
        String s2 = String.join("\n"
                                , "<http://example.org/subject> <http://example.org/predicate> <http://example.org/object>."
                                , "<http://example.org/subject> <http://example.org/predicate2> \"some text\"."
                                , "<http://example.org/subject2> <http://example.org/predicate> _:id."
                                , "<http://example.org/subject2> <http://example.org/predicate> \"more text\"^^<http://example.org/datatype>.");
        // @formatter:on
        assertEquals(4, parseCount(s));

        Model m = parseToModelRdfJson(s);
        Model m2 = parseToModelNTriples(s2);
        assertTrue(m.isIsomorphicWith(m2));
    }

    @Test
    public void rdfjson_read_bnode_identity() {
        String s = "{ \"_:id\" : { \"http://example.org/predicate\" : [ { \"type\" : \"bnode\" , \"value\" : \"_:id\" } ] } }";
        String s2 = "_:id <http://example.org/predicate> _:id .";

        assertEquals(1, parseCount(s));

        Model m = parseToModelRdfJson(s);
        Model m2 = parseToModelNTriples(s2);
        assertTrue(m.isIsomorphicWith(m2));
    }

    @Test
    public void rdfjson_read_bnode_identity2() {
        // @formatter:off
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\" : \"bnode\" , \"value\" : \"_:id\" } ] , \"http://example.org/predicate2\" : [ { \"type\" : \"bnode\" , \"value\" : \"_:id\" } ] } }";
        String s2 = String.join("\n" , "<http://example.org/subject> <http://example.org/predicate> _:id ."
                  , "<http://example.org/subject> <http://example.org/predicate2> _:id .");
        // @formatter:on

        assertEquals(2, parseCount(s));

        Model m = parseToModelRdfJson(s);
        Model m2 = parseToModelNTriples(s2);
        assertTrue(m.isIsomorphicWith(m2));
    }

    @Test
    public void rdfjson_invalid_empty_string() {
        String s = "";
        parseException(ExFatal.class, ()->parseCount(s));
    }

    @Test
    public void rdfjson_invalid_unterminated_graph() {
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\" : \"uri\" , \"value\" : \"http://example.org/object\" } ] } ";
        parseException(ExFatal.class, ()->parseCount(s));
    }

    @Test
    public void rdfjson_invalid_missing_colon_after_subject() {
        String s = "{ \"http://example.org/subject\"  { \"http://example.org/predicate\" : [ { \"type\" : \"uri\" , \"value\" : \"http://example.org/object\" } ] } }";
        parseException(ExFatal.class, ()->parseCount(s));
    }

    @Test
    public void rdfjson_invalid_missing_colon_after_predicate() {
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\"  [ { \"type\" : \"uri\" , \"value\" : \"http://example.org/object\" } ] } }";
        parseException(ExFatal.class, ()->parseCount(s));
    }

    @Test
    public void rdfjson_invalid_missing_colon_in_object() {
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\"  \"uri\" , \"value\" : \"http://example.org/object\" } ] } }";
        parseException(ExFatal.class, ()->parseCount(s));
    }

    @Test
    public void rdfjson_invalid_unterminated_predicateobjectlist_object() {
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\" : \"uri\" , \"value\" : \"http://example.org/object\" } ]";
        parseException(ExFatal.class, ()->parseCount(s));
    }

    @Test
    public void rdfjson_invalid_unterminated_objectlist_array() {
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\" : \"uri\" , \"value\" : \"http://example.org/object\" } } }";
        parseException(ExFatal.class, ()->parseCount(s));
    }

    @Test
    public void rdfjson_invalid_unterminated_object_object() {
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\" : \"uri\" , \"value\" : \"http://example.org/object\"";
        parseException(ExFatal.class, ()->parseCount(s));
    }

    @Test
    public void rdfjson_invalid_trailing_comma_in_object() {
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\" : \"uri\" , \"value\" : \"http://example.org/object\" , } ] } }";
        parseException(ExFatal.class, ()->parseCount(s));
    }

    @Test
    public void rdfjson_invalid_trailing_comma_after_subject() {
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\" : \"uri\" , \"value\" : \"http://example.org/object\" } ] } , }";
        parseException(ExFatal.class, ()->parseCount(s));
    }

    @Test
    public void rdfjson_invalid_trailing_comma_after_predicate() {
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\" : \"uri\" , \"value\" : \"http://example.org/object\" } ] , } }";
        parseException(ExFatal.class, ()->parseCount(s));
    }

    @Test
    public void rdfjson_invalid_property_names_in_object() {
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"name\" : \"value\" } ] } }";
        parseException(ExFatal.class, ()->parseCount(s));
    }

    @Test
    public void rdfjson_invalid_lang_and_datatype_in_object() {
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\" : \"literal\" , \"value\" : \"some text\" , \"lang\" : \"en\" , \"datatype\" : \"http://example.org/datatype\" } ] } }";
        parseException(ExFatal.class, ()->parseCount(s));
    }

    @Test
    public void rdfjson_invalid_lang_and_datatype_in_object2() {
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\" : \"literal\" , \"value\" : \"some text\" , \"datatype\" : \"http://example.org/datatype\" , \"lang\" : \"en\" } ] } }";
        parseException(ExFatal.class, ()->parseCount(s));
    }

    @Test
    public void rdfjson_invalid_repeated_property_type_in_object() {
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\" : \"uri\" , \"type\" : \"uri\" , \"value\" : \"http://example.org/object\" } ] } }";
        parseException(ExFatal.class, ()->parseCount(s));
    }

    @Test
    public void rdfjson_invalid_repeated_property_value_in_object() {
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\" : \"uri\" , \"value\" : \"http://example.org/object\" , \"value\" : \"http://example.org/object\" } ] } }";
        parseException(ExFatal.class, ()->parseCount(s));
    }

    @Test
    public void rdfjson_invalid_value_type_in_object() {
        String s = "{ \"http://example.org/subject\" : { \"http://example.org/predicate\" : [ { \"type\" : \"other\" , \"value\" : \"http://example.org/object\" } ] } }";
        parseException(ExFatal.class, ()->parseCount(s));
    }

    @Test
    public void rdfjson_invalid_array_toplevel() {
        String s = "[]";
        parseException(ExFatal.class, ()->parseCount(s));
    }

    @Test
    public void rdfjson_invalid_trailing_content() {
        String s = "{}{}";
        parseException(ExFatal.class, ()->parseCount(s));
    }

    @Test
    public void rdfjson_invalid_tokenizer() {
        byte b[] = StrUtils.asUTF8bytes("");
        ByteArrayInputStream in = new ByteArrayInputStream(b);
        Tokenizer tokenizer = TokenizerText.create().source(in).build();
        StreamRDFCounting sink = StreamRDFLib.count();
        // IllegalArgumentException - not a TokenizerJSON.
        assertThrows(IllegalArgumentException.class, ()->new LangRDFJSON(tokenizer, RiotLib.dftProfile(), sink));
    }

    private <T extends Throwable> T parseException(Class<T> exClass, Executable action) {
        return assertThrowsExactly(exClass, action);
    }


    private long parseCount(String string) {
        return ParserTests.parseCount(Lang.RDFJSON, string);
    }

    private Model parseToModelNTriples(String string) {
        StringReader r = new StringReader(string);
        Model model = ModelFactory.createDefaultModel();
        RDFDataMgr.read(model, r, null, RDFLanguages.NTRIPLES);
        return model;
    }

    private Model parseToModelRdfJson(String string) {
        return ParserTests.parser().fromString(string).lang(Lang.RDFJSON).toModel();
    }
}
