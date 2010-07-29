package org.phenoscape.obd.model;

public class GeneTerm extends DefaultTerm {
    
    private String fullName;

    public GeneTerm(int nodeID, Integer sourceID) {
        super(nodeID, sourceID);
    }

    public String getFullName() {
        return this.fullName;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
    }

}
