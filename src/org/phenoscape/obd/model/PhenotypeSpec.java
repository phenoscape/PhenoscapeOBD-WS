package org.phenoscape.obd.model;

public class PhenotypeSpec {
    
    private String entityID;
    private String qualityID;
    private String relatedEntityID;
    private boolean includeEntityParts;
    
    public PhenotypeSpec(String entityID, String qualityID, String relatedEntityID, boolean includeEntityParts) {
        this.entityID = entityID;
        this.qualityID = qualityID;
        this.relatedEntityID = relatedEntityID;
        this.includeEntityParts = includeEntityParts;
    }
    
    public PhenotypeSpec() {
        this(null, null, null, false);
    }

    public void setEntityID(String entityID) {
        this.entityID = entityID;
    }

    public void setQualityID(String qualityID) {
        this.qualityID = qualityID;
    }

    public void setRelatedEntityID(String relatedEntityID) {
        this.relatedEntityID = relatedEntityID;
    }

    public void setIncludeEntityParts(boolean includeEntityParts) {
        this.includeEntityParts = includeEntityParts;
    }

    public String getEntityID() {
        return this.entityID;
    }

    public String getQualityID() {
        return this.qualityID;
    }

    public String getRelatedEntityID() {
        return this.relatedEntityID;
    }

    public boolean includeEntityParts() {
        return this.includeEntityParts;
    }

}
