package org.phenoscape.obd.model;

import java.util.Set;

public class PhenotypeVariationSetsResult {

    private final Set<PhenotypeVariationSet> sets;
    private final String parentTaxonID;

    public PhenotypeVariationSetsResult(String parent, Set<PhenotypeVariationSet> variationSets) {
        this.sets = variationSets;
        this.parentTaxonID = parent;
    }

    public Set<PhenotypeVariationSet> getVariationSets() {
        return this.sets;
    }

    public String getParentTaxonID() {
        return this.parentTaxonID;
    }

}
