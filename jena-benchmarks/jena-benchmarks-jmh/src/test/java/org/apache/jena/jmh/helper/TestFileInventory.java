package org.apache.jena.jmh.helper;

public abstract class TestFileInventory {

    public final static String TTL_CHEESE = "cheeses-0.1.ttl";
    public final static String RDF_PIZZA_OWL = "pizza-owl.rdf";
    public final static String RDF_CITATIONS = "citations.rdf";

    public final static String NT_GZ_BSBM_1M = "bsbm-1m.nt.g";
    public final static String NT_GZ_BSBM_5M = "bsbm-5m.nt.g";
    public final static String NT_GZ_BSBM_25M = "bsbm-25m.nt.g";

    public final static String XML_BSBM_5M = "bsbm-5m.xml";
    public final static String XML_BSBM_25M = "bsbm-25m.xml";

    public final static String RDF_EQUIPMENT_CORE_PROFILE_RDFS_AUGMENTED_V2_4_15_4SEP2020 = "EquipmentProfileCoreRDFSAugmented-v2_4_15-4Sep2020.rdf";
    public final static String RDF_STEADY_STATE_HYPOTHESIS_PROFILE_RDFS_AUGMENTED_V2_4_15_4SEP2020 = "SteadyStateHypothesisProfileRDFSAugmented-v2_4_15-4Sep2020.rdf";
    public final static String RDF_STATE_VARIABLE_PROFILE_RDFS_AUGMENTED_V2_4_15_4SEP2020 = "StateVariableProfileRDFSAugmented-v2_4_15-4Sep2020.rdf";
    public final static String RDF_TOPOLOGY_PROFILE_RDFS_AUGMENTED_V2_4_15_4SEP2020 = "TopologyProfileRDFSAugmented-v2_4_15-4Sep2020.rdf";

    public final static String XML_REAL_GRID_V2_EQ = "CGMES_v2.4.15_RealGridTestConfiguration_EQ_V2.xml";
    public final static String XML_REAL_GRID_V2_SSH = "CGMES_v2.4.15_RealGridTestConfiguration_SSH_V2.xml";
    public final static String XML_REAL_GRID_V2_TP = "CGMES_v2.4.15_RealGridTestConfiguration_TP_V2.xml";
    public final static String XML_REAL_GRID_V2_SV = "CGMES_v2.4.15_RealGridTestConfiguration_SV_V2.xml";

    public final static String XML_XXX_CGMES_EQ = "xxx_CGMES_EQ.xml";
    public final static String XML_XXX_CGMES_SSH = "xxx_CGMES_SSH.xml";
    public final static String XML_XXX_CGMES_TP = "xxx_CGMES_TP.xml";

    public static String getFilePath(String fileName) {
        switch (fileName) {
            case TTL_CHEESE:
                return "../testing/cheeses-0.1.ttl";
            case RDF_PIZZA_OWL:
                return "../testing/pizza.owl.rdf";
            case RDF_CITATIONS:
                return "../testing/citations.rdf";

            case NT_GZ_BSBM_1M:
                return "../testing/BSBM/bsbm-1m.nt.gz";
            case NT_GZ_BSBM_5M:
                return "../testing/BSBM/bsbm-5m.nt.gz";
            case NT_GZ_BSBM_25M:
                return "../testing/BSBM/bsbm-25m.nt.gz";

            case XML_BSBM_5M:
                return "../testing/BSBM/bsbm-5m.xml";
            case XML_BSBM_25M:
                return "../testing/BSBM/bsbm-25m.xml";

            case RDF_EQUIPMENT_CORE_PROFILE_RDFS_AUGMENTED_V2_4_15_4SEP2020:
                return "C:\\rd\\bewegungsdaten-demo\\shared\\ENTSOE_RDF\\src\\main\\resources\\CGMES\\v2.4.15\\CGMES2415_Components_2020\\RDFS\\EquipmentProfileCoreRDFSAugmented-v2_4_15-4Sep2020.rdf";
            case RDF_STEADY_STATE_HYPOTHESIS_PROFILE_RDFS_AUGMENTED_V2_4_15_4SEP2020:
                return "C:\\rd\\bewegungsdaten-demo\\shared\\ENTSOE_RDF\\src\\main\\resources\\CGMES\\v2.4.15\\CGMES2415_Components_2020\\RDFS\\SteadyStateHypothesisProfileRDFSAugmented-v2_4_15-4Sep2020.rdf";
            case RDF_STATE_VARIABLE_PROFILE_RDFS_AUGMENTED_V2_4_15_4SEP2020:
                return "C:\\rd\\bewegungsdaten-demo\\shared\\ENTSOE_RDF\\src\\main\\resources\\CGMES\\v2.4.15\\CGMES2415_Components_2020\\RDFS\\StateVariableProfileRDFSAugmented-v2_4_15-4Sep2020.rdf";
            case RDF_TOPOLOGY_PROFILE_RDFS_AUGMENTED_V2_4_15_4SEP2020:
                return "C:\\rd\\bewegungsdaten-demo\\shared\\ENTSOE_RDF\\src\\main\\resources\\CGMES\\v2.4.15\\CGMES2415_Components_2020\\RDFS\\TopologyProfileRDFSAugmented-v2_4_15-4Sep2020.rdf";

            case XML_REAL_GRID_V2_EQ:
                return "C:/rd/CGMES/TestConfigurations_packageCASv2.0/RealGrid/CGMES_v2.4.15_RealGridTestConfiguration_EQ_V2.xml";
            case XML_REAL_GRID_V2_SSH:
                return "C:/rd/CGMES/TestConfigurations_packageCASv2.0/RealGrid/CGMES_v2.4.15_RealGridTestConfiguration_SSH_V2.xml";
            case XML_REAL_GRID_V2_TP:
                return "C:/rd/CGMES/TestConfigurations_packageCASv2.0/RealGrid/CGMES_v2.4.15_RealGridTestConfiguration_TP_V2.xml";
            case XML_REAL_GRID_V2_SV:
                return "C:/rd/CGMES/TestConfigurations_packageCASv2.0/RealGrid/CGMES_v2.4.15_RealGridTestConfiguration_SV_V2.xml";

            case XML_XXX_CGMES_EQ:
                return "C:/temp/res_test/xxx_CGMES_EQ.xml";
            case XML_XXX_CGMES_SSH:
                return "C:/temp/res_test/xxx_CGMES_SSH.xml";
            case XML_XXX_CGMES_TP:
                return "C:/temp/res_test/xxx_CGMES_TP.xml";


            //            ,
//            "C:\\temp\\bsbm-25m.xml"

            default:
                throw new IllegalArgumentException("Unknown fileName: " + fileName);
        }
    }
}
