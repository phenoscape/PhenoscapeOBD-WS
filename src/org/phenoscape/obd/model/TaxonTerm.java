package org.phenoscape.obd.model;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class TaxonTerm extends DefaultTerm {

    private boolean isExtinct = false;
    private TaxonTerm parent = null;
    private Set<TaxonTerm> children = new HashSet<TaxonTerm>();
    private Term rank = null;

    public TaxonTerm(int nodeID) {
        super(nodeID);
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
        return parent;
    }
    
    public void setParent(TaxonTerm parent) {
        this.parent = parent;
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

}
