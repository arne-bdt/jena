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

// Encoding in Thrift of RDF terms and other items 
// for Graph, Datasets, Result Set and Patches

namespace java org.apache.jena.riot.thrift3.wire

// ==== RDF Term Definitions 

struct RDF_IRI {
1: required i32 iri
}

# A prefix name (abbrev for an IRI)
struct RDF_PrefixName {
1: required i32 prefix;
2: required i32 localName;
}

struct RDF_BNode {
  // Maybe support (or even insist) on a global unique identifier e.g. UUID
  // long mostSig
  // long leastSig
1: required i32 label
}

// Common abbreviated for datatypes and other URIs?
// union with additional values. 

struct RDF_Literal {
1: required string lex;
2: optional i32  langtag;
3: optional i32  datatype;          // Either 3 or 4 but UNION is heavy.
4: optional RDF_PrefixName dtPrefix ;   // datatype as prefix name
}

struct RDF_VAR {
1: required i32 name;
}

struct RDF_ANY { }

struct RDF_UNDEF { }

struct RDF_REPEAT { }

union RDF_Term {
1: RDF_IRI          iri
2: RDF_BNode        bnode
3: RDF_Literal      literal     # Full form lexical form/datatype/langtag
4: RDF_PrefixName   prefixName 
5: RDF_VAR          variable
6: RDF_ANY          any
7: RDF_UNDEF        undefined
8: RDF_REPEAT       repeat
9: RDF_Triple       tripleTerm  # RDF-star
}

// === Stream RDF items 

struct RDF_Triple {
1: required     RDF_Term S
2: required     RDF_Term P
3: required     RDF_Term O
}

struct RDF_Quad {
1: required     RDF_Term S
2: required     RDF_Term P
3: required     RDF_Term O
4: optional     RDF_Term G
}

# Prefix declaration
struct RDF_PrefixDecl {
1: required i32 prefix;
2: required i32 uri;
}

union RDF_StreamUnion {
# No base - no relative URI resolution.
1: RDF_PrefixDecl   prefixDecl
2: RDF_Triple       triple
3: RDF_Quad         quad
}

struct RDF_StreamRow {
1: RDF_StreamUnion row
2: optional list<string>    strings
}

// ==== SPARQL Result Sets

struct RDF_VarTuple {
1: list<RDF_VAR> vars
2: optional list<string> strings
}

struct RDF_DataTuple {
1: list<RDF_Term> row
2: optional list<string> strings
}

// ==== RDF Patch

enum PatchTxn { TX, TC, TA , Segment }

struct Patch_Prefix_Add {
1: optional RDF_Term graphNode;
2: required i32 prefix;
3: required i32 iriStr;
}

struct Patch_Prefix_Del {
1: optional RDF_Term graphNode;
2: required i32 prefix;
}

struct Patch_Header {
1: required i32 name;
2: required RDF_Term value;
}

struct Patch_Data_Add {
1: required RDF_Term s;
2: required RDF_Term p;
3: required RDF_Term o;
4: optional RDF_Term g;
}

struct Patch_Data_Del {
1: required RDF_Term s;
2: required RDF_Term p;
3: required RDF_Term o;
4: optional RDF_Term g;
}

union RDF_Patch_Union {
1: Patch_Header       header;
2: Patch_Data_Add     dataAdd;
3: Patch_Data_Del     dataDel;
4: Patch_Prefix_Add   prefixAdd;
5: Patch_Prefix_Del   prefixDel;
6: PatchTxn           txn;
}

struct RDF_Patch_Row {
1: RDF_Patch_Union          row;
2: optional list<string>    strings;
}

// Local Variables:
// tab-width: 2
// indent-tabs-mode: nil
// comment-default-style: "//"
// End:
