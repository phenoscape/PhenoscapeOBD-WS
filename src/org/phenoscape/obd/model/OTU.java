package org.phenoscape.obd.model;

public class OTU extends DefaultTerm {
    
    private TaxonTerm taxon;
    
    public OTU(int nodeID) {
        super(nodeID, null);
    }
    
    public TaxonTerm getTaxon() {
        return this.taxon;
    }
    
    public void setTaxon(TaxonTerm taxon) {
        this.taxon = taxon;
    }
    
    @Override
    public int hashCode() {
        return this.getUID().hashCode();
    }
    
    @Override
    public boolean equals(Object other) {
        if (other instanceof OTU) {
            final OTU otherOTU = (OTU)other;
            return (this.getUID().equals(otherOTU.getUID()));
        }
        return false;
    }
    
}
