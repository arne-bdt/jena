package org.apache.jena.cimxml.rdfs;

import org.apache.jena.cimxml.CimVersion;
import org.apache.jena.cimxml.graph.CimProfile;
import org.apache.jena.datatypes.RDFDatatype;
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


    static Map<String, RDFDatatype> initPrimitiveToRDFDatatypeMapUsingXSDDatatypesOnly() {
        var map = new HashMap<String, RDFDatatype>();
        map.put("Base64Binary", XSDDatatype.XSDbase64Binary);
        map.put("Boolean", XSDDatatype.XSDboolean);
        map.put("Byte", XSDDatatype.XSDbyte);
        map.put("Date", XSDDatatype.XSDdate);
        map.put("DateTime", XSDDatatype.XSDdateTime);
        map.put("DateTimeStamp", XSDDatatype.XSDdateTimeStamp);
        map.put("Day", XSDDatatype.XSDgDay);
        map.put("DayTimeDuration", XSDDatatype.XSDdayTimeDuration);
        map.put("Decimal", XSDDatatype.XSDdecimal);
        map.put("Double", XSDDatatype.XSDdouble);
        map.put("Duration", XSDDatatype.XSDduration);
        map.put("Float", XSDDatatype.XSDfloat);
        map.put("HexBinary", XSDDatatype.XSDhexBinary);
        map.put("Int", XSDDatatype.XSDint);
        map.put("Integer", XSDDatatype.XSDinteger);
        map.put("IRI", XSDDatatype.XSDstring);
        map.put("LangString", RDFLangString.rdfLangString);
        map.put("Long", XSDDatatype.XSDlong);
        map.put("Month", XSDDatatype.XSDgMonth);
        map.put("MonthDay", XSDDatatype.XSDgMonthDay);
        map.put("NegativeInteger", XSDDatatype.XSDnegativeInteger);
        map.put("NonNegativeInteger", XSDDatatype.XSDnonNegativeInteger);
        map.put("NonPositiveInteger", XSDDatatype.XSDnonPositiveInteger);
        map.put("PositiveInteger", XSDDatatype.XSDpositiveInteger);
        map.put("String", XSDDatatype.XSDstring);
        map.put("StringFixedLanguage", XSDDatatype.XSDstring);
        map.put("StringIRI", XSDDatatype.XSDstring);
        map.put("Time", XSDDatatype.XSDtime);
        map.put("UnsignedByte", XSDDatatype.XSDunsignedByte);
        map.put("UnsignedInt", XSDDatatype.XSDunsignedInt);
        map.put("UnsignedLong", XSDDatatype.XSDunsignedLong);
        map.put("UnsignedShort", XSDDatatype.XSDunsignedShort);
        map.put("URI", XSDDatatype.XSDanyURI);
        map.put("UUID", XSDDatatype.XSDstring);
        map.put("Version", XSDDatatype.XSDstring);
        map.put("Year", XSDDatatype.XSDgYear);
        map.put("YearMonth", XSDDatatype.XSDgYearMonth);
        map.put("YearMonthDuration", XSDDatatype.XSDyearMonthDuration);
        return map;
    }

    private final Map<Set<Node>, CimProfile> multiVersionIriProfiles = new ConcurrentHashMap<>();
    private final Map<Node, CimProfile> singleVersionIriProfiles = new ConcurrentHashMap<>();
    private final Map<CimVersion, CimProfile> headerProfiles = new ConcurrentHashMap<>();
    private final Map<CimProfile, Map<Node, PropertyInfo>> profilePropertiesCache = new ConcurrentHashMap<>();
    private final Map<Set<CimProfile>, Map<Node, PropertyInfo>> profileSetPropertiesCache = new ConcurrentHashMap<>();
    private final Map<String, RDFDatatype> primitiveToRDFDatatypeMap;

    public final ErrorHandler errorHandler;

    public CimProfileRegistryStd() {
        this(ErrorHandlerFactory.errorHandlerStd);
    }

    public CimProfileRegistryStd(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
        this.primitiveToRDFDatatypeMap = initPrimitiveToRDFDatatypeMapUsingXSDDatatypesOnly();
    }

    private final static Query typedPropertiesQuery = QueryFactory.create("""
           PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
           PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
           PREFIX cims: <http://iec.ch/TC57/1999/rdf-schema-extensions-19990926#>
           
           SELECT ?rdfType ?property ?cimDatatype ?primitiveType ?referenceType
           WHERE
           {
             {
             	?property rdfs:domain ?rdfType;
                 		  rdfs:range ?referenceType;
               OPTIONAL {
           		?property cims:AssociationUsed ?associationUsed
           	}
           	FILTER(!BOUND(?associationUsed) || ?associationUsed = "Yes")
             }
             UNION
             {
               ?property rdfs:domain ?rdfType;
                         cims:dataType ?cimDatatype.
               {
                 ?cimDatatype cims:stereotype "CIMDatatype".
                 []  rdfs:domain ?cimDatatype;
                     rdfs:label ?label;
                     #rdfs:label "value";
                     cims:dataType/cims:stereotype "Primitive";
                     cims:dataType/rdfs:label ?primitiveType.
                 FILTER (!bound(?label) ||  str(?label) = "value")
               }
               UNION
               {
                 ?cimDatatype cims:stereotype "Primitive";
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
        profilePropertiesCache.put(cimProfile, getTypedProperties(cimProfile));
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
                for (var entrySet : multiVersionIriProfiles.entrySet()) {
                    if(entrySet.getKey().contains(owlVersionIRI)) {
                        foundInMulti = true;
                        set.add(entrySet.getValue());
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

    @Override
    public Map<String, RDFDatatype> getPrimitiveToRDFDatatypeMapping() {
        return Collections.unmodifiableMap(primitiveToRDFDatatypeMap);
    }

    @Override
    public void registerPrimitiveType(String cimPrimitiveTypeName, RDFDatatype rdfDatatype) {
        Objects.requireNonNull(cimPrimitiveTypeName);
        Objects.requireNonNull(rdfDatatype);
        primitiveToRDFDatatypeMap.put(cimPrimitiveTypeName, rdfDatatype);
    }

    private Map<Node, PropertyInfo> getTypedProperties(Graph g) {
        final var map = new HashMap<Node, PropertyInfo>(1024);
        QueryExec.graph(g)
                .query(typedPropertiesQuery)
                .select()
                .forEachRemaining(vars -> { //?class ?property ?primitiveType ?referenceType
                    final var rdfType = vars.get("rdfType");
                    final var property = vars.get("property");
                    final var cimDatatype = vars.get("property");
                    final var primitiveType = vars.get("primitiveType");
                    final var referenceType = vars.get("referenceType");
                    map.put(property, new PropertyInfo(
                            rdfType,
                            property,
                            cimDatatype,
                            primitiveType != null
                                    ? getXsdDatatype(primitiveType.getLiteralLexicalForm())
                                    : null,
                            referenceType));
                });
        return Collections.unmodifiableMap(map);
    }

    private RDFDatatype getXsdDatatype(String primitiveType) {
        var dt = primitiveToRDFDatatypeMap.get(primitiveType);
        if(dt != null)
            return dt;
        errorHandler.warning("Unknown mapping from CIM primitive'" + primitiveType + "' to XSD datatype. Using xsd:string as fallback.", -1,-1);
        return XSDDatatype.XSDstring;
    }
}
