package org.phenoscape.obd.model;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class TaxonTerm extends DefaultTerm {

    private boolean isExtinct = false;
    private TaxonTerm parent = null;
    private Set<TaxonTerm> children = new HashSet<TaxonTerm>();
    private Term rank = null;
    private TaxonTerm taxonomicFamily = null;
    private TaxonTerm taxonomicOrder = null;
    private int speciesCount = 0;

    public TaxonTerm(int nodeID, Integer sourceID) {
        super(nodeID, sourceID);
    }
    public boolean isExtinct() {
        return isExtinct;
    }
    public void setExtinct(boolean isExtinct) {
        this.isExtinct = isExtinct;
    }
    /**
     * Returns a TaxonTerm representing this taxon's parent taxon. If it is
     * null, this taxon may either not have a parent or this object may not know about it.
     */
    public TaxonTerm getParent() {
        return this.parent;
    }
    
    public void setParent(TaxonTerm parent) {
        this.parent = parent;
    }
    
    public TaxonTerm getTaxonomicFamily() {
        return this.taxonomicFamily;
    }
    
    public void setTaxonomicFamily(TaxonTerm taxonomicClass) {
        this.taxonomicFamily = taxonomicClass;
    }
    
    public TaxonTerm getTaxonomicOrder() {
        return this.taxonomicOrder;
    }
    
    public void setTaxonomicOrder(TaxonTerm taxonomicOrder) {
        this.taxonomicOrder = taxonomicOrder;
    }

    /**
     * Returns a set of TaxonTerms representing this taxon's children taxa. 
     * If the set is empty, this taxon may either have no children or this object doesn't know about them.
     */
    public Set<TaxonTerm> getChildren() {
        return Collections.unmodifiableSet(children);
    }

    public void addChild(TaxonTerm child) {
        this.children.add(child);
    }
    
    public Term getRank() {
        return rank;
    }
    
    public void setRank(Term rank) {
        this.rank = rank;
    }
 
    public int getSpeciesCount() {
        return this.speciesCount;
    }
    
    public void setSpeciesCount(int count) {
        this.speciesCount = count;
    }

}
