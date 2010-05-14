package org.phenoscape.obd.model;

import java.util.Set;

public interface Term {

    public String getUID();

    public void setUID(String uid);

    public String getLabel();

    public void setLabel(String label);

    public String getDefinition();

    public void setDefinition(String definition);

    public Set<Synonym> getSynonyms();

    public void addSynonym(Synonym synonym);
    
}
