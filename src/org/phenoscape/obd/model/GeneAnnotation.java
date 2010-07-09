package org.phenoscape.obd.model;

public class GeneAnnotation {

    private GeneTerm gene;
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

}
