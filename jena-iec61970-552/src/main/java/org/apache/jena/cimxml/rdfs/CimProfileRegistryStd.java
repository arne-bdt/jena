package org.apache.jena.cimxml.rdfs;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.cimxml.CimVersion;
import org.apache.jena.cimxml.datatypes.SemVer2_0DataType;
import org.apache.jena.cimxml.datatypes.UuidDataType;
import org.apache.jena.cimxml.graph.CimProfile;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.datatypes.xsd.impl.RDFLangString;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.riot.system.ErrorHandler;
import org.apache.jena.riot.system.ErrorHandlerFactory;
import org.apache.jena.sparql.exec.QueryExec;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class CimProfileRegistryStd implements CimProfileRegistry {

    static {
        // Register CIM specific datatypes
        TypeMapper.getInstance().registerDatatype(SemVer2_0DataType.INSTANCE);
        TypeMapper.getInstance().registerDatatype(UuidDataType.INSTANCE);
    }

    private final Map<Set<Node>, CimProfile> multiVersionIriProfiles = new ConcurrentHashMap<>();
    private final Map<Node, CimProfile> singleVersionIriProfiles = new ConcurrentHashMap<>();
    private final Map<CimVersion, CimProfile> headerProfiles = new ConcurrentHashMap<>();
    private final Map<CimProfile, Map<Node, PropertyInfo>> profilePropertiesCache = new ConcurrentHashMap<>();
    private final Map<Set<CimProfile>, Map<Node, PropertyInfo>> profileSetPropertiesCache = new ConcurrentHashMap<>();

    public final ErrorHandler errorHandler;

    public CimProfileRegistryStd() {
        this(ErrorHandlerFactory.errorHandlerStd);
    }

    public CimProfileRegistryStd(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    private final static Query typedPropertiesQuery = QueryFactory.create("""
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX cims: <http://iec.ch/TC57/1999/rdf-schema-extensions-19990926#>
            
            SELECT ?class ?property ?primitiveType ?referenceType
            WHERE
            {
              {
              	?property rdfs:domain ?class;
                  		  rdfs:range ?referenceType.
                OPTIONAL {
                    ?property cims:AssociationUsed ?associationUsed.
                }
                FILTER(!BOUND(?associationUsed) || ?associationUsed = "Yes")
              }
              UNION
              {
                ?property rdfs:domain ?class;
                          cims:dataType ?dataType.
                {
                  ?dataType cims:stereotype "CIMDatatype".
                  []  rdfs:domain ?dataType;
                      rdfs:label ?label;
                      #rdfs:label "value";
                      cims:dataType/cims:stereotype "Primitive";
                      cims:dataType/rdfs:label ?primitiveType.
                  FILTER (!bound(?label) ||  str(?label) = "value")
                }
                UNION
                {
                  ?dataType   cims:stereotype "Primitive";
                              rdfs:label ?primitiveType.
                }
              }
            }
            """);

    @Override
    public void register(CimProfile cimProfile) {
        if(cimProfile.isHeaderProfile()) {
            final var cimVersion = cimProfile.getCIMVersion();
            if(cimVersion == CimVersion.NO_CIM)
                throw new IllegalArgumentException("Header profile must have a valid CIM version.");
            if(headerProfiles.containsKey(cimVersion))
                throw new IllegalArgumentException("Header profile for CIM version " + cimVersion + " is already registered.");
            headerProfiles.put(cimVersion, cimProfile);
            profilePropertiesCache.put(cimProfile, getTypedProperties(cimProfile, errorHandler));
            return;
        }

        var owlVersionIRIs = cimProfile.getOwlVersionIRIs();
        if(owlVersionIRIs == null || owlVersionIRIs.isEmpty())
            throw new IllegalArgumentException("Profile ontology must have at least one owlVersionIRI.");

        if(owlVersionIRIs.size() == 1) {
            var iri = owlVersionIRIs.iterator().next();
            if(singleVersionIriProfiles.containsKey(iri))
                throw new IllegalArgumentException("Profile ontology with owlVersionIRI " + iri + " is already registered.");
            singleVersionIriProfiles.put(iri, cimProfile);
        } else {
            if(multiVersionIriProfiles.containsKey(owlVersionIRIs))
                throw new IllegalArgumentException("Profile ontology with owlVersionIRIs " + owlVersionIRIs + " is already registered.");
            multiVersionIriProfiles.put(owlVersionIRIs, cimProfile);
        }
        profilePropertiesCache.put(cimProfile, getTypedProperties(cimProfile, errorHandler));
    }

    @Override
    public boolean containsProfile(Set<Node> owlVersionIRIs) {
        if(owlVersionIRIs == null || owlVersionIRIs.isEmpty())
            throw new IllegalArgumentException("At least one profile owlVersionIRI must be provided.");
        for(var iri : owlVersionIRIs) {
            if(!singleVersionIriProfiles.containsKey(iri)) {
                var foundInMulti = false;
                for(var registeredVersionIRIs: multiVersionIriProfiles.keySet()) {
                    if(registeredVersionIRIs.contains(iri))
                        foundInMulti = true;
                }
                if(!foundInMulti)
                    return false;
            }
        }
        return true;
    }

    @Override
    public boolean containsHeaderProfile(CimVersion version) {
        if(version == CimVersion.NO_CIM)
            throw new IllegalArgumentException("CIM version must be valid.");
        return headerProfiles.containsKey(version);
    }

    @Override
    public Set<CimProfile> getRegisteredProfiles() {
        return profilePropertiesCache.keySet();
    }

    @Override
    public Map<Node, PropertyInfo> getPropertiesAndDatatypes(Set<Node> owlVersionIRIs) {
        if(owlVersionIRIs == null || owlVersionIRIs.isEmpty())
            throw new IllegalArgumentException("At least one profile owlVersionIRI must be provided.");

        if(owlVersionIRIs.size() == 1) {
            var versionIRI = owlVersionIRIs.iterator().next();
            if(singleVersionIriProfiles.containsKey(versionIRI)) {
                var profile = singleVersionIriProfiles.get(owlVersionIRIs.iterator().next());
                return profilePropertiesCache.get(profile);
            }
        }

        var profile = multiVersionIriProfiles.get(owlVersionIRIs);
        if(profile != null)
            return profilePropertiesCache.get(profile);

        var set = new HashSet<CimProfile>();
        for(var owlVersionIRI : owlVersionIRIs) {
            final var p = singleVersionIriProfiles.get(owlVersionIRI);
            if(p == null) {
                var foundInMulti = false;
                for (Set<Node> versionSet : multiVersionIriProfiles.keySet()) {
                    var multiP = multiVersionIriProfiles.get(versionSet);
                    if(multiP != null) {
                        foundInMulti = true;
                        set.add(multiP);
                    }
                }
                if (!foundInMulti)
                    return null;
            } else {
                set.add(p);
            }
        }
        if(set.size() == 1)
            return profilePropertiesCache.get(set.iterator().next());

        Map<Node, PropertyInfo> properties = profileSetPropertiesCache.get(set);
        if(properties != null)
            return properties;

        properties = new HashMap<>(1024);
        for(var p : set) {
            properties.putAll(profilePropertiesCache.get(p));
        }
        properties = Collections.unmodifiableMap(properties);
        profileSetPropertiesCache.put(set, properties);
        return properties;
    }

    @Override
    public Map<Node, PropertyInfo> getHeaderPropertiesAndDatatypes(CimVersion version) {
        Objects.requireNonNull(version);
        if(version == CimVersion.NO_CIM)
            throw new IllegalArgumentException("CIM version must be valid.");
        final var profile = headerProfiles.get(version);
        if(profile == null)
            return null;
        return profilePropertiesCache.get(profile);
    }

    private static Map<Node, PropertyInfo> getTypedProperties(Graph g, ErrorHandler errorHandler) {
        final var map = new HashMap<Node, PropertyInfo>(1024);
        QueryExec.graph(g)
                .query(typedPropertiesQuery)
                .select()
                .forEachRemaining(vars -> { //?class ?property ?primitiveType ?referenceType
                    final var clazz = vars.get("class");
                    final var property = vars.get("property");
                    final var primitiveType = vars.get("primitiveType");
                    final var referenceType = vars.get("referenceType");
                    if(referenceType != null) {
                        map.put(property, new PropertyInfo(clazz, property, null, referenceType));
                    } else {
                        map.put(property, new PropertyInfo(clazz, property, getDataType(primitiveType.getLiteralLexicalForm(), errorHandler), null));
                    }

                });
        return Collections.unmodifiableMap(map);
    }

    private static RDFDatatype getDataType(String primitiveType, ErrorHandler errorHandler) {
        switch (primitiveType) {
            case "Base64Binary":
                return XSDDatatype.XSDbase64Binary;
            case "Boolean":
                return XSDDatatype.XSDboolean;
            case "Byte":
                return XSDDatatype.XSDbyte;
            case "Date":
                return XSDDatatype.XSDdate;
            case "DateTime":
                return XSDDatatype.XSDdateTime;
            case "DateTimeStamp":
                return XSDDatatype.XSDdateTimeStamp;
            case "Day":
                return XSDDatatype.XSDgDay;
            case "DayTimeDuration":
                return XSDDatatype.XSDdayTimeDuration;
            case "Decimal":
                return XSDDatatype.XSDdecimal;
            case "Double":
                return XSDDatatype.XSDdouble;
            case "Duration":
                return XSDDatatype.XSDduration;
            case "Float":
                return XSDDatatype.XSDfloat;
            case "HexBinary":
                return XSDDatatype.XSDhexBinary;
            case "Int":
                return XSDDatatype.XSDint;
            case "Integer":
                return XSDDatatype.XSDinteger;
            case "IRI":
                return XSDDatatype.XSDanyURI;
            case "LangString":
                return RDFLangString.rdfLangString;
            case "Long":
                return XSDDatatype.XSDlong;
            case "Month":
                return XSDDatatype.XSDgMonth;
            case "MonthDay":
                return XSDDatatype.XSDgMonthDay;
            case "NegativeInteger":
                return XSDDatatype.XSDnegativeInteger;
            case "NonNegativeInteger":
                return XSDDatatype.XSDnonNegativeInteger;
            case "NonPositiveInteger":
                return XSDDatatype.XSDnonPositiveInteger;
            case "PositiveInteger":
                return XSDDatatype.XSDpositiveInteger;
            case "String", "StringFixedLanguage", "StringIRI":
                return XSDDatatype.XSDstring;
            case "Time":
                return XSDDatatype.XSDtime;
            case "UnsignedByte":
                return XSDDatatype.XSDunsignedByte;
            case "UnsignedInt":
                return XSDDatatype.XSDunsignedInt;
            case "UnsignedLong":
                return XSDDatatype.XSDunsignedLong;
            case "UnsignedShort":
                return XSDDatatype.XSDunsignedShort;
            case "URI":
                return XSDDatatype.XSDanyURI;
            case "UUID":
                return UuidDataType.INSTANCE;
            case "Version":
                return SemVer2_0DataType.INSTANCE;
            case "Year":
                return XSDDatatype.XSDgYear;
            case "YearMonth":
                return XSDDatatype.XSDgYearMonth;
            case "YearMonthDuration":
                return XSDDatatype.XSDyearMonthDuration;
            default:
                errorHandler.warning("Unknown mapping from '" + primitiveType + "' to XSD datatype. Using xsd:string as fallback.", -1,-1);
                return XSDDatatype.XSDstring;
        }
    }
}
