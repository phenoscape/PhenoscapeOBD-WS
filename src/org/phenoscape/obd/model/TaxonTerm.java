package org.phenoscape.obd.model;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class TaxonTerm extends Term {

    private boolean isExtinct = false;
    private TaxonTerm parent = null;
    private Set<TaxonTerm> children = new HashSet<TaxonTerm>();
    private Term rank = null;

    public boolean isExtinct() {
        return isExtinct;
    }
    public void setExtinct(boolean isExtinct) {
        this.isExtinct = isExtinct;
    }
    public TaxonTerm getParent() {
        return parent;
    }
    
    public void setParent(TaxonTerm parent) {
        this.parent = parent;
    }

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
