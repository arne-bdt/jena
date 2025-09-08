# Apache Jena IEC 61970-552 CIMXML Parser

## Overview

The `jena-iec61970-552` module provides a specialized parser and data structures for handling IEC 61970-552 CIMXML (Common Information Model XML) files within the Apache Jena RDF framework. This module enables parsing, manipulation, and querying of power system models that conform to the IEC 61970-552 standard, commonly used in the energy sector for exchanging power system network data.

## Features

- **CIMXML Parser**: Specialized StAX-based parser optimized for CIMXML documents
- **CIM Version Support**: Supports CIM versions 16, 17, and 18
- **Profile Management**: Registry system for CIM profile ontologies with datatype resolution
- **Model Types**: Support for both FullModel and DifferenceModel as per IEC 61970-552
- **Graph Operations**: Specialized graph implementations for efficient handling of large CIM models
- **UUID Handling**: Automatic normalization of CIM UUIDs (with and without underscores/dashes)
- **Datatype Resolution**: Automatic resolution of CIM datatypes from registered profiles

## Quick Start

### Basic Usage

```java
import org.apache.jena.cimxml.parser.CimXmlParser;
import org.apache.jena.cimxml.sparql.core.CimDatasetGraph;
import java.nio.file.Path;

// Create a parser instance
CimXmlParser parser = new CimXmlParser();

// Parse a CIMXML file
CimDatasetGraph dataset = parser.parseCimModel(Path.of("model.xml"));

// Check model type
if (dataset.isFullModel()) {
    // Access the model header and body
    var header = dataset.getModelHeader();
    var body = dataset.getBody();
    
    // Get model metadata
    var modelId = header.getModel();
    var profiles = header.getProfiles();
}
```

### Working with CIM Profiles

```java
// Register CIM profile ontologies for datatype resolution
CimProfile profile = parser.parseAndRegisterCimProfile(Path.of("Equipment.rdf"));

// Parse CIMXML with profile-aware datatype resolution
CimDatasetGraph dataset = parser.parseCimModel(Path.of("model.xml"));
```

### Handling Difference Models

```java
// Parse a difference model
CimDatasetGraph diffModel = parser.parseCimModel(Path.of("difference.xml"));

if (diffModel.isDifferenceModel()) {
    // Access difference components
    var forwardDiffs = diffModel.getForwardDifferences();
    var reverseDiffs = diffModel.getReverseDifferences();
    var preconditions = diffModel.getPreconditions();
    
    // Apply differences to a base model
    CimDatasetGraph baseModel = parser.parseCimModel(Path.of("base.xml"));
    Graph resultModel = diffModel.differenceModelToFullModel(baseModel);
}
```

## Architecture

### Core Components

#### 1. Parser (`org.apache.jena.cimxml.parser`)
- `CimXmlParser`: Main entry point for parsing CIMXML files
- `ReaderCIMXML_StAX_SR`: StAX-based XML reader implementation
- `ParserCIMXML_StAX_SR`: Core parsing logic with CIMXML-specific handling

#### 2. Graph Structures (`org.apache.jena.cimxml.graph`)
- `CimProfile`: Represents CIM profile ontologies (versions 16, 17, 18)
- `CimModelHeader`: Wrapper for model header information
- `FastDeltaGraph`: Efficient implementation for difference models
- `DisjointMultiUnion`: Union graph without duplicate elimination

#### 3. Dataset (`org.apache.jena.cimxml.sparql.core`)
- `CimDatasetGraph`: Extended DatasetGraph with CIM-specific operations
- `LinkedCimDatasetGraph`: Implementation supporting multiple named graphs

#### 4. Profile Registry (`org.apache.jena.cimxml.rdfs`)
- `CimProfileRegistry`: Interface for managing CIM profiles
- `CimProfileRegistryStd`: Standard implementation with caching

### Data Model

The module organizes CIMXML data into distinct graphs:

**FullModel Structure:**
- Model Header Graph (named: `urn:FullModel`)
- Body Graph (default graph)

**DifferenceModel Structure:**
- Model Header Graph (named: `urn:DifferenceModel`)
- Forward Differences Graph
- Reverse Differences Graph
- Preconditions Graph (optional)

## IEC 61970-552 Compliance

### Supported Features

- ✅ Processing instruction: `<?iec61970-552 version="x.x"?>`
- ✅ FullModel and DifferenceModel types
- ✅ Model header metadata (profiles, supersedes, dependentOn)
- ✅ parseType="Statements" for difference model containers
- ✅ UUID normalization (underscore prefix handling)
- ✅ CIM namespace version detection

### CIM Versions

| Version | Namespace URI | Status |
|---------|--------------|--------|
| CIM 16 | `http://iec.ch/TC57/2013/CIM-schema-cim16#` | ✅ Supported |
| CIM 17 | `http://iec.ch/TC57/CIM100#` | ✅ Supported |
| CIM 18 | `https://cim.ucaiug.io/ns#` | ✅ Supported |

## Advanced Usage

### Custom Error Handling

```java
import org.apache.jena.riot.system.ErrorHandler;
import org.apache.jena.riot.system.ErrorHandlerFactory;

// Create parser with custom error handler
ErrorHandler errorHandler = ErrorHandlerFactory.errorHandlerStrict;
CimXmlParser parser = new CimXmlParser(errorHandler);
```

### Profile Registry Management

```java
import org.apache.jena.cimxml.rdfs.CimProfileRegistry;
import org.apache.jena.cimxml.rdfs.CimProfileRegistryStd;

// Create and configure registry
CimProfileRegistry registry = new CimProfileRegistryStd();

// Register custom primitive type mappings
registry.registerPrimitiveType("Voltage", XSDDatatype.XSDdouble);

// Register profiles
CimProfile profile = CimProfile.wrap(profileGraph);
registry.register(profile);

// Query registered profiles
Set<CimProfile> profiles = registry.getRegisteredProfiles();
```

### Working with Model Headers

```java
CimModelHeader header = dataset.getModelHeader();

// Get model metadata
Node modelUri = header.getModel();
Set<Node> profileUris = header.getProfiles();
Set<Node> supersededModels = header.getSupersedes();
Set<Node> dependencies = header.getDependentOn();

// Check model type
boolean isFullModel = header.isFullModel();
boolean isDiffModel = header.isDifferenceModel();
```

### SPARQL Queries on CIM Data

```java
import org.apache.jena.query.*;

// Create SPARQL query
String queryString = """
    PREFIX cim: <http://iec.ch/TC57/CIM100#>
    SELECT ?equipment ?name
    WHERE {
        ?equipment a cim:Equipment ;
                   cim:IdentifiedObject.name ?name .
    }
    """;

Query query = QueryFactory.create(queryString);

// Execute query on CIM dataset
try (QueryExecution qexec = QueryExecutionFactory.create(query, dataset)) {
    ResultSet results = qexec.execSelect();
    while (results.hasNext()) {
        QuerySolution solution = results.nextSolution();
        // Process results
    }
}
```

## Performance Considerations

### Memory Optimization

The module uses specialized graph implementations optimized for CIM data:

- `GraphMem2Roaring`: Roaring bitmap-based indexing for large graphs
- `IndexingStrategy.LAZY_PARALLEL`: Deferred parallel index construction
- `FastDeltaGraph`: Efficient difference application without materialization

### Large File Handling

```java
// Use buffered file channel for large files
Path largeCimFile = Path.of("large_model.xml");
CimDatasetGraph dataset = parser.parseCimModel(largeCimFile);
// Internally uses BufferedFileChannelInputStream with optimal buffer sizing
```

## Testing

The module includes comprehensive test coverage:

- W3C RDF/XML conformance tests
- CIM-specific parsing tests
- Profile version detection tests
- Difference model application tests

Run tests with:
```bash
mvn test -pl jena-iec61970-552
```

## Dependencies

```xml
<dependency>
    <groupId>org.apache.jena</groupId>
    <artifactId>jena-iec61970-552</artifactId>
    <version>5.6.0-SNAPSHOT</version>
</dependency>
```

Required dependencies:
- Apache Jena ARQ
- Woodstox StAX parser
- Aalto XML processor

## Limitations

1. **Browser Storage**: The module does not support localStorage/sessionStorage APIs
2. **Profile Dependencies**: Datatype resolution requires pre-registered CIM profiles
3. **Memory Usage**: Large models may require JVM heap tuning
4. **Streaming**: Full streaming is not supported; documents are loaded into memory

## Contributing

Contributions are welcome! Please ensure:
1. All tests pass
2. Code follows Apache Jena coding standards
3. JavaDoc is complete for public APIs
4. Changes are documented in release notes

## License

Licensed under the Apache License, Version 2.0. See LICENSE file for details.

## References

- [IEC 61970-552 Standard](https://webstore.iec.ch/publication/25939)
- [CIM Users Group](https://cimug.ucaiug.org/)
- [Apache Jena Documentation](https://jena.apache.org/)
- [CGMES (Common Grid Model Exchange Standard)](https://www.entsoe.eu/digital/cim/)

## Support

For issues and questions:
- GitHub Issues: [Apache Jena GitHub](https://github.com/apache/jena)
- Mailing List: users@jena.apache.org
- Stack Overflow: Tag with `apache-jena` and `cim`