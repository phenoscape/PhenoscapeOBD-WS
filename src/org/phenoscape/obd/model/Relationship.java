package org.phenoscape.obd.model;

/**
 * Describes a relationship one term has to another. This class does not define whether the other 
 * object is the subject or object of the link - that depends on how this Relationship is 
 * used by a given LinkedTerm.
 */
public class Relationship {

    private Term predicate;
    private Term other;
    
    public Relationship() {}
    
    public Relationship(Term predicate, Term other) {
        this.predicate = predicate;
        this.other = other;
    }

    public Term getPredicate() {
        return predicate;
    }

    public void setPredicate(Term predicate) {
        this.predicate = predicate;
    }

    public Term getOther() {
        return other;
    }

    public void setOther(Term other) {
        this.other = other;
    }

}
