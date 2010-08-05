package org.phenoscape.obd.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.phenoscape.obd.model.PhenotypeSpec;

public class AnnotationsQueryConfig {

    public static enum SORT_COLUMN {TAXON, GENE, GENE_FULLNAME, ENTITY, QUALITY, RELATED_ENTITY, FAMILY, ORDER, PUBLICATION};
    private SORT_COLUMN sortColumn = SORT_COLUMN.ENTITY;
    private int limit = 0;
    private int index = 0;
    private boolean sortDescending = false;
    private final List<String> taxonIDs = new ArrayList<String>();
    private final List<String> geneIDs = new ArrayList<String>();
    private final List<PhenotypeSpec> phenotypes = new ArrayList<PhenotypeSpec>();
    private final List<String> publicationIDs = new ArrayList<String>();
    private boolean includeInferredAnnotations = false;
    private boolean matchAllPhenotypes = false;
    private boolean matchAllPublications = false;
    private boolean matchAllTaxa = false;

    public boolean matchAllPhenotypes() {
        return matchAllPhenotypes;
    }

    public void setMatchAllPhenotypes(boolean matchAllPhenotypes) {
        this.matchAllPhenotypes = matchAllPhenotypes;
    }

    public boolean matchAllPublications() {
        return matchAllPublications;
    }

    public void setMatchAllPublications(boolean matchAllPublications) {
        this.matchAllPublications = matchAllPublications;
    }

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
    
    public void setIncludeInferredAnnotations(boolean include) {
        this.includeInferredAnnotations = include;
    }

    public List<String> getTaxonIDs() {
        return Collections.unmodifiableList(this.taxonIDs);
    }

    public void addTaxonID(String taxonID) {
        this.taxonIDs.add(taxonID);
    }

    public void addAllTaxonIDs(List<String> taxonIDs) {
        this.taxonIDs.addAll(taxonIDs);
    }
    
    public List<String> getGeneIDs() {
        return Collections.unmodifiableList(this.geneIDs);
    }

    public void addGeneID(String geneID) {
        this.geneIDs.add(geneID);
    }

    public void addAllGeneIDs(List<String> geneIDs) {
        this.geneIDs.addAll(geneIDs);
    }
    
    public List<String> getPublicationIDs() {
        return Collections.unmodifiableList(this.publicationIDs);
    }

    public void addPublicationID(String publicationID) {
        this.publicationIDs.add(publicationID);
    }

    public void addAllPublicationIDs(List<String> publicationIDs) {
        this.publicationIDs.addAll(publicationIDs);
    }

    public List<PhenotypeSpec> getPhenotypes() {
        return Collections.unmodifiableList(this.phenotypes);
    }

    public void addPhenotype(PhenotypeSpec phenotype) {
        this.phenotypes.add(phenotype);
    }

    public void addAllPhenotypes(List<PhenotypeSpec> phenotypes) {
        this.phenotypes.addAll(phenotypes);
    }

    public boolean matchAllTaxa() {
        return this.matchAllTaxa;
    }
    
    public void setMatchAllTaxa(boolean matchAll) {
        this.matchAllTaxa = matchAll;
    }

}
