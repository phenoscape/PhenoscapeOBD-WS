package org.phenoscape.obd.model;

public class GeneAnnotation {

    private GeneTerm gene;
    private Term genotype;
    private Term genotypeClass;
    private Term publication;
    private Term entity;
    private Term quality;
    private Term relatedEntity;

    public GeneTerm getGene() {
        return gene;
    }
    
    public void setGene(GeneTerm gene) {
        this.gene = gene;
    }
    
    public Term getEntity() {
        return entity;
    }
    
    public void setEntity(Term entity) {
        this.entity = entity;
    }
    
    public Term getQuality() {
        return quality;
    }
    
    public void setQuality(Term quality) {
        this.quality = quality;
    }
    
    public Term getRelatedEntity() {
        return relatedEntity;
    }
    
    public void setRelatedEntity(Term relatedEntity) {
        this.relatedEntity = relatedEntity;
    }

    public Term getGenotype() {
        return genotype;
    }

    public void setGenotype(Term genotype) {
        this.genotype = genotype;
    }

    public Term getGenotypeClass() {
        return genotypeClass;
    }

    public void setGenotypeClass(Term genotypeClass) {
        this.genotypeClass = genotypeClass;
    }

    public Term getPublication() {
        return publication;
    }

    public void setPublication(Term publication) {
        this.publication = publication;
    }

}
