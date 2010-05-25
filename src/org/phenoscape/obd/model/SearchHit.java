package org.phenoscape.obd.model;

public class SearchHit {

    private final Term term;
    public static enum MatchType { NAME, SYNONYM, DEFINITION; }
    private final String text;
    private final MatchType type;

    public SearchHit(Term term, String text, MatchType matchType) {
        this.term = term;
        this.text = text;
        this.type = matchType;
    }

    public Term getHit() {
        return this.term;
    }

    public String getPrimaryText() {
        return this.term.getLabel();
    }

    public String getMatchText() {
        return this.text;
    }

    public MatchType getMatchType() {
        return this.type;
    }

    @Override
    public String toString() {
        return this.getPrimaryText() + " as " + this.getMatchText() + ": {" + this.getMatchType() + "}";
    }

}
