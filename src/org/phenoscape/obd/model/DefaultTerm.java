package org.phenoscape.obd.model;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class DefaultTerm implements LinkedTerm {

    private String uid;
    private String label;
    private String definition;
    private Set<Synonym> synonyms = new HashSet<Synonym>();
    private Set<Relationship> subjectLinks = new HashSet<Relationship>();
    private Set<Relationship> objectLinks = new HashSet<Relationship>();
    private final int nodeID;
        
    public DefaultTerm(int nodeID) {
        this.nodeID = nodeID;
    }
    
    public int getNodeID() {
        return this.nodeID;
    }

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
    
    /**
     * The Relationships in which this term is the subject of the link - this term's "parents".
     */
    public Set<Relationship> getSubjectLinks() {
        return Collections.unmodifiableSet(this.subjectLinks);
    }
    
    public void addSubjectLink(Relationship relationship) {
        this.subjectLinks.add(relationship);
    }
    
    /**
     * The Relationships in which this term is the object of the link - this term's "children".
     */
    public Set<Relationship> getObjectLinks() {
        return Collections.unmodifiableSet(this.objectLinks);
    }
    
    public void addObjectLink(Relationship relationship) {
        this.objectLinks.add(relationship);
    }

}
