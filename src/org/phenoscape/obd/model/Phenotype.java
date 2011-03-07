package org.phenoscape.obd.model;

public class Phenotype {
    
    private Term entity;
    private Term quality;
    private Term relatedEntity;

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
