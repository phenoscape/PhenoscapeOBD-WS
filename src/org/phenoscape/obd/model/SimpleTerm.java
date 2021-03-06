package org.phenoscape.obd.model;

import java.util.Collections;
import java.util.Set;

public class SimpleTerm implements Term {

    private final String id;
    private final String label;
    private final Term source;

    public SimpleTerm(String uid, String label) {
        this.id = uid;
        this.label = label;
        this.source = null;
    }
    
    public SimpleTerm(String uid, String label, Term source) {
        this.id = uid;
        this.label = label;
        this.source = source;
    }

    @Override
    public void addSynonym(Synonym synonym) {}
    
    @Override
    public void addXref(Term xref) {}

    @Override
    public String getComment() {
        return null;
    }

    @Override
    public String getDefinition() {
        return null;
    }

    @Override
    public String getLabel() {
        return this.label;
    }

    @Override
    public Set<Synonym> getSynonyms() {
        return Collections.emptySet();
    }
    
    @Override
    public Set<Term> getXrefs() {
        return Collections.emptySet();
    }

    @Override
    public String getUID() {
        return this.id;
    }

    @Override
    public void setComment(String comment) {
        throw new UnsupportedOperationException("Comment is final");
    }

    @Override
    public void setDefinition(String definition) {
        throw new UnsupportedOperationException("Definition is final");
    }

    @Override
    public void setLabel(String label) {
        throw new UnsupportedOperationException("Label is final");
    }

    @Override
    public void setUID(String uid) {
        throw new UnsupportedOperationException("UID is final");
    }

    @Override
    public Term getSource() {
        return this.source;
    }

    @Override
    public void setSource(Term source) {
        throw new UnsupportedOperationException("Changing source term is not supported");
    }

}
