package org.phenoscape.obd.model;

import java.util.Set;

public interface LinkedTerm extends Term {
    
    /**
     * The Relationships in which this term is the subject of the link - this term's "parents".
     */
    public Set<Relationship> getSubjectLinks();
    
    public void addSubjectLink(Relationship relationship);
    
    /**
     * The Relationships in which this term is the object of the link - this term's "children".
     */
    public Set<Relationship> getObjectLinks();
    
    public void addObjectLink(Relationship relationship);

}
