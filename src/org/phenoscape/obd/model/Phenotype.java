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
    
    @Override
    public int hashCode() {
        return this.entity.getUID().hashCode() ^ this.quality.getUID().hashCode() ^ (this.relatedEntity == null ? 0 : this.relatedEntity.getUID().hashCode());  
    }
    
    @Override
    public boolean equals(Object other) {
        if (other instanceof Phenotype) {
            final Phenotype otherPhenotype = (Phenotype)other;
            final boolean eq = this.entity.getUID().equals(otherPhenotype.entity.getUID()) && this.quality.getUID().equals(otherPhenotype.quality.getUID());
            if (eq) {
                if (this.relatedEntity != null) {
                    if (otherPhenotype.relatedEntity != null) {
                        return this.relatedEntity.getUID().equals(otherPhenotype.relatedEntity.getUID());
                    } else {
                        return false;
                    }
                } else {
                    return otherPhenotype.relatedEntity == null;
                } 
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

}
