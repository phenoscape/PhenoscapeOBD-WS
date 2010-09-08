package org.phenoscape.obd.model;

public class Vocab {

    public static class OBO {

        public static final String IS_A = "OBO_REL:is_a";
        public static final String COMMENT = "oboInOwl:comment";
        public static final String INHERES_IN = "OBO_REL:inheres_in";
        public static final String INHERES_IN_PART_OF = "OBO_REL:inheres_in_part_of";
        public static final String INSTANCE_OF = "OBO_REL:instance_of";
        public static final String TOWARDS = "OBO_REL:towards";
        public static final String HAS_DBXREF = "oboInOwl:hasDbXref";
    }

    public static class TAO {

        public static final String NAMESPACE = "teleost_anatomy";
    }

    public static class TTO {

        public static final String NAMESPACE = "teleost-taxonomy";
        public static final String CYPRINIFORMES = "TTO:1360";
        public static final String SILURIFORMES = "TTO:1380";
        public static final String CHARACIFORMES = "TTO:1370";
        public static final String GYMNOTIFORMES = "TTO:1390";
        public static final String GONORYNCHIFORMES = "TTO:1350";
        public static final String CLUPEIFORMES = "TTO:1340";
        public static final String EUTELEOSTEI = "TTO:254";
        public static final String COMMONNAME = "COMMONNAME";

        public static final String[] HIGHER_LEVEL_TAXA = {CHARACIFORMES, CLUPEIFORMES, CYPRINIFORMES, GONORYNCHIFORMES, GYMNOTIFORMES, SILURIFORMES, EUTELEOSTEI};
    }

    public static class PATO {

        public static final String NAMESPACE = "quality";
    }

    public static class GO {

        public static final String NAMESPACE = "gene_ontology";
        public static final String BP_NAMESPACE = "biological_process";
        public static final String MF_NAMESPACE = "molecular_function";
        public static final String CC_NAMESPACE = "cellular_component";

    }

    public static class ZFIN {
        public static final String GENE_NAMESPACE = "zfin_gene";
        public static final String FULL_NAME_SYNONYM_CATEGORY = "FULLNAME";
    }

    public static class PHENOSCAPE {
        public static final String PUB_NAMESPACE = "phenoscape_pub";
        public static final String PUBLICATION = "PHENOSCAPE:Publication";
    }

    public static class CDAO {
        public static final String CHARACTER = "cdao:Character";
        public static final String CHARACTER_STATE = "cdao:CharacterStateDomain";
        public static final String OTU = "cdao:TU";
        public static final String HAS_PHENOTYPE = "cdao:has_Phenotype";

    }

}
