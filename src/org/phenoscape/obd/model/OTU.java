package org.phenoscape.obd.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class OTU extends DefaultTerm {
    
    private TaxonTerm taxon;
    private List<Specimen> specimens = new ArrayList<Specimen>();
    
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
    
    public void addSpecimen(Specimen specimen) {
        this.specimens.add(specimen);
    }
    
    public void addAllSpecimens(Collection<Specimen> specimens) {
        this.specimens.addAll(specimens);
    }

    public List<Specimen> getSpecimens() {
        return Collections.unmodifiableList(this.specimens);
    }
    
}
