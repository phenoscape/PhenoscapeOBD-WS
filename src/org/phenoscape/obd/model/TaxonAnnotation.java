package org.phenoscape.obd.model;

public class TaxonAnnotation {

    private TaxonTerm taxon;
    private Term entity;
    private Term quality;
    private Term relatedEntity;
    private Term publication;
    private Character character;
    private Term state;
    private Term otu;

    public TaxonTerm getTaxon() {
        return this.taxon;
    }

    public void setTaxon(TaxonTerm taxon) {
        this.taxon = taxon;
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
    
    public Character getCharacter() {
        return this.character;
    }
    
    public void setCharacter(Character character) {
        this.character = character;
    }
    
    public Term getState() {
        return this.state;
    }
    
    public void setState(Term state) {
        this.state = state;
    }

    public Term getPublication() {
        return publication;
    }

    public void setPublication(Term publication) {
        this.publication = publication;
    }

    public Term getOtu() {
        return otu;
    }

    public void setOtu(Term otu) {
        this.otu = otu;
    }

}
