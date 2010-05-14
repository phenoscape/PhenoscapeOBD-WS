package org.phenoscape.obd.model;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class Term {

    private String uid;
    private String label;
    private String definition;
    private Set<Synonym> synonyms = new HashSet<Synonym>();

    public String getUID() {
        return uid;
    }

    public void setUID(String uid) {
        this.uid = uid;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getDefinition() {
        return definition;
    }

    public void setDefinition(String definition) {
        this.definition = definition;
    }

    public Set<Synonym> getSynonyms() {
        return Collections.unmodifiableSet(this.synonyms);
    }

    public void addSynonym(Synonym synonym) {
        this.synonyms.add(synonym);
    }

}
