package org.phenoscape.obd.model;

public class Vocab {
    
    public static class OBO {

        public static final String IS_A = "OBO_REL:is_a";
        public static final String COMMENT = "oboInOwl:comment";
       
    }
    
    public static class TAO {

        public static final String NAMESPACE = "teleost_anatomy";
    }
    
    public static class TTO {

        public static final String NAMESPACE = "teleost-taxonomy";
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
        public static final String GENE_NAMESPACE = "zfin-gene";
    }

}
