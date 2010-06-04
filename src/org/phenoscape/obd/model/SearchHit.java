package org.phenoscape.obd.model;

/**
 * A search match to a term in the Knowledgebase, describing the term that was matched and also which text matched the input text.
 * @author jim
 *
 */
public class SearchHit {

    private final Term term;
    /**
     * The type of match, indicating whether the search matched the term's name or some other text associated with the term.
     */
    public static enum MatchType {
        /**
         * The input matched the term's name.
         */
        NAME, 
        /**
         * The input matched a synonym of the term.
         */
        SYNONYM
    }
    private final String matchText;
    private final MatchType type;

    /**
     * Create a SearchHit representing a match to a particular term.
     * @param term The matched term.
     * @param matchText The textual entity that matched the input text, e.g. the term name or a synonym.
     * @param matchType The type of match, e.g. to a term name or to a synonym.
     */
    public SearchHit(Term term, String matchText, MatchType matchType) {
        this.term = term;
        this.matchText = matchText;
        this.type = matchType;
    }

    /**
     * The actual Term that was matched, whether by its own name or by a synonym.
     */
    public Term getHit() {
        return this.term;
    }

    /**
     * The label for the matched term object, rather than the text that matched the input.
     */
    public String getPrimaryText() {
        return this.term.getLabel();
    }

    /**
     * The text, such as the term name or synonym, that matched the search input.
     */
    public String getMatchText() {
        return this.matchText;
    }

    /**
     * The type of match that occurred.
     */
    public MatchType getMatchType() {
        return this.type;
    }

    @Override
    public String toString() {
        return this.getPrimaryText() + " as " + this.getMatchText() + ": {" + this.getMatchType() + "}";
    }

}
