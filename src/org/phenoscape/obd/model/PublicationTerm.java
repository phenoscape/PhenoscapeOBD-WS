package org.phenoscape.obd.model;

public class PublicationTerm extends DefaultTerm {
    
    private String abstractText;
    private String citation;
    private String doi;

    public PublicationTerm(int nodeID, Integer sourceID) {
        super(nodeID, sourceID);
    }

    public String getAbstractText() {
        return abstractText;
    }

    public void setAbstractText(String abstractText) {
        this.abstractText = abstractText;
    }

    public String getCitation() {
        return citation;
    }

    public void setCitation(String citation) {
        this.citation = citation;
    }

    public String getDoi() {
        return doi;
    }

    public void setDoi(String doi) {
        this.doi = doi;
    }
    
}
