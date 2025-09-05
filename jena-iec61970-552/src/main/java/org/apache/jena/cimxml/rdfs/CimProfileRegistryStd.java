package org.apache.jena.cimxml.rdfs;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.cimxml.CimVersion;
import org.apache.jena.cimxml.graph.CimProfile;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.datatypes.xsd.impl.RDFLangString;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.exec.QueryExec;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class CimProfileRegistryStd implements CimProfileRegistry {

    private final Map<Node, CimProfile> dataProfiles = new ConcurrentHashMap<>();
    private final Map<CimVersion, CimProfile> headerProfiles = new ConcurrentHashMap<>();
    private final Map<CimProfile, Map<Node, PropertyInfo>> profilePropertiesCache = new ConcurrentHashMap<>();
    private final Map<Set<CimProfile>, Map<Node, PropertyInfo>> profileSetPropertiesCache = new ConcurrentHashMap<>();


    private final static Query typedPropertiesQuery = QueryFactory.create("""
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX cims: <http://iec.ch/TC57/1999/rdf-schema-extensions-19990926#>
            
            SELECT ?class ?property ?primitiveType ?referenceType
            WHERE
            {
              {
              	?property rdfs:domain ?class;
                  		  rdfs:range ?referenceType;
                		  cims:AssociationUsed "Yes";
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
            profilePropertiesCache.put(cimProfile, getTypedProperties(cimProfile));
            return;
        }

        var owlVersionIRIs = cimProfile.getOwlVersionIRIs();
        if(owlVersionIRIs == null || owlVersionIRIs.isEmpty())
            throw new IllegalArgumentException("Profile ontology must have at least one owlVersionIRI.");
        for(var iri : owlVersionIRIs) {
            if(dataProfiles.containsKey(iri))
                throw new IllegalArgumentException("Profile ontology with owlVersionIRI " + iri + " is already registered.");
        }
        var profileAndProperties = Pair.of(cimProfile, getTypedProperties(cimProfile));
        for(var iri : owlVersionIRIs) {
            dataProfiles.put(iri, cimProfile);
        }
        profilePropertiesCache.put(cimProfile, getTypedProperties(cimProfile));
    }

    @Override
    public void unregister(CimProfile cimProfile) {
        if(cimProfile.isHeaderProfile()) {
            final var cimVersion = cimProfile.getCIMVersion();
            if(cimVersion == CimVersion.NO_CIM)
                throw new IllegalArgumentException("Header profile must have a valid CIM version.");
            if(!headerProfiles.containsKey(cimVersion))
                throw new IllegalArgumentException("Header profile for CIM version " + cimVersion + " is not registered.");
            headerProfiles.remove(cimVersion);
            profilePropertiesCache.remove(cimProfile);
            return;
        }

        var owlVersionIRIs = cimProfile.getOwlVersionIRIs();
        if(owlVersionIRIs == null || owlVersionIRIs.isEmpty())
            throw new IllegalArgumentException("Profile ontology must have at least one owlVersionIRI.");
        for(var iri : owlVersionIRIs) {
            if(!dataProfiles.containsKey(iri))
                throw new IllegalArgumentException("Profile ontology with owlVersionIRI " + iri + " is not registered.");
        }
        for(var iri : owlVersionIRIs) {
            dataProfiles.remove(iri);
        }
        profilePropertiesCache.remove(cimProfile);
        var setsToRemove = new ArrayList<Set<CimProfile>>();
        for (Set<CimProfile> cimProfiles : profileSetPropertiesCache.keySet()) {
            if(cimProfiles.contains(cimProfile)) {
                setsToRemove.add(cimProfiles);
            }
        }
        for (Set<CimProfile> set : setsToRemove) {
            profileSetPropertiesCache.remove(set);
        }
    }

    @Override
    public boolean containsProfile(Set<Node> owlVersionIRIs) {
        if(owlVersionIRIs == null || owlVersionIRIs.isEmpty())
            throw new IllegalArgumentException("At least one profile owlVersionIRI must be provided.");
        for(var iri : owlVersionIRIs) {
            if(!dataProfiles.containsKey(iri))
                return false;
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
        var set = new HashSet<CimProfile>();
        for(var owlVersionIRI : owlVersionIRIs) {
            var profile = dataProfiles.get(owlVersionIRI);
            if(profile == null)
                throw new IllegalArgumentException("Profile ontology with owlVersionIRI " + owlVersionIRI + " is not registered.");
            set.add(profile);
        }
        if(set.size() == 1)
            return profilePropertiesCache.get(set.iterator().next());

        var properties = profileSetPropertiesCache.get(getRegisteredProfiles());

        if(properties != null)
            return properties;

        properties = new HashMap<Node, PropertyInfo>(1024);
        for(var profile : set) {
            properties.putAll(profilePropertiesCache.get(profile));
        }
        properties = Collections.unmodifiableMap(properties);
        profileSetPropertiesCache.put(set, properties);
        return properties;
    }

    @Override
    public Map<Node, PropertyInfo> getHeaderPropertiesAndDatatypes(CimVersion version) {
        if(version == CimVersion.NO_CIM)
            throw new IllegalArgumentException("CIM version must be valid.");
        final var profile = headerProfiles.get(version);
        return profilePropertiesCache.get(profile);
    }

    private static Map<Node, PropertyInfo> getTypedProperties(Graph g) {
        final var map = new HashMap<Node, PropertyInfo>(1024);
        QueryExec.graph(g)
                .query(typedPropertiesQuery)
                .select()
                .forEachRemaining(vars -> { //?class ?property ?primitiveType ?referenceType
                    final var clazz = vars.get("class");
                    final var property = vars.get("property");
                    final var primitiveType = vars.get("primitiveType").getLiteralLexicalForm();
                    final var referenceType = vars.get("referenceType");
                    if("IRI".equals(primitiveType)) {
                        map.put(property, new PropertyInfo(clazz, property, null, referenceType));
                    } else {
                        map.put(property, new PropertyInfo(clazz, property, getDataType(primitiveType), null));
                    }

                });
        return Collections.unmodifiableMap(map);
    }

    private static RDFDatatype getDataType(String primitiveType) {
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
            case "String":
                return XSDDatatype.XSDstring;
            case "StringIRI":
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
            case "Year":
                return XSDDatatype.XSDgYear;
            case "YearMonth":
                return XSDDatatype.XSDgYearMonth;
            case "YearMonthDuration":
                return XSDDatatype.XSDyearMonthDuration;
            default:
                throw new IllegalArgumentException("The type '" + primitiveType + "' is not yet supported.");
        }
    }
}
