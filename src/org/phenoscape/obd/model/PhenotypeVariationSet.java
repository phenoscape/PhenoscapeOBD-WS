package org.phenoscape.obd.model;

import java.util.HashSet;
import java.util.Set;

public class PhenotypeVariationSet {
    
    final Set<String> taxa = new HashSet<String>();
    final Set<Phenotype> phenotypes = new HashSet<Phenotype>();
    
    public PhenotypeVariationSet(Set<String> taxa, Set<Phenotype> phenotypes) {
        this.taxa.addAll(taxa);
        this.phenotypes.addAll(phenotypes);
    }
    
    public Set<String> getTaxa() {
        return this.taxa;
    }

    public Set<Phenotype> getPhenotypes() {
        return this.phenotypes;
    }
    
}
