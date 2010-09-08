 package org.phenoscape.obd.model;

import java.util.Set;

public interface Term {

    public String getUID();

    public void setUID(String uid);

    public String getLabel();

    public void setLabel(String label);

    public String getDefinition();

    public void setDefinition(String definition);
    
    public String getComment();
    
    public void setComment(String comment);

    public Set<Synonym> getSynonyms();

    public void addSynonym(Synonym synonym);
    
    public String getSourceUID();
    
    public void setSourceUID(String sourceUID);
    
    public Set<Term> getXrefs();
    
    public void addXref(Term xref);
    
}
