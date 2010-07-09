package org.phenoscape.obd.query;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.phenoscape.obd.model.PhenotypeSpec;

public class GeneAnnotationsQueryConfig {

    public static enum SORT_COLUMN {GENE, ENTITY, QUALITY, RELATED_ENTITY};
    private SORT_COLUMN sortColumn;
    private int limit = 0;
    private int index = 0;
    private boolean sortDescending = false;
    private final Set<String> geneIDs = new HashSet<String>();
    private final Set<PhenotypeSpec> phenotypes = new HashSet<PhenotypeSpec>();
    private boolean includeInferredAnnotations = false;

    public SORT_COLUMN getSortColumn() {
        return this.sortColumn;
    }

    public void setSortColumn(SORT_COLUMN sortColumn) {
        this.sortColumn = sortColumn;
    }

    public int getLimit() {
        return this.limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public int getIndex() {
        return this.index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public boolean sortDescending() {
        return this.sortDescending;
    }

    public void setSortDescending(boolean sortDescending) {
        this.sortDescending = sortDescending;
    }

    public boolean includeInferredAnnotations() {
        return this.includeInferredAnnotations;
    }

    public void setIncludeInferredAnnotations(boolean includeInferredAnnotations) {
        this.includeInferredAnnotations = includeInferredAnnotations;
    }

    public Set<String> getGeneIDs() {
        return Collections.unmodifiableSet(this.geneIDs);
    }

    public void addGeneID(String geneID) {
        this.geneIDs.add(geneID);
    }

    public void addAllGeneIDs(Set<String> geneIDs) {
        this.geneIDs.addAll(geneIDs);
    }

    public Set<PhenotypeSpec> getPhenotypes() {
        return Collections.unmodifiableSet(this.phenotypes);
    }

    public void addPhenotype(PhenotypeSpec phenotype) {
        this.phenotypes.add(phenotype);
    }

    public void addAllPhenotypes(Set<PhenotypeSpec> phenotypes) {
        this.phenotypes.addAll(phenotypes);
    }

}
