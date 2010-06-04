package org.phenoscape.obd.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * The result of a textual search for terms used in autocomplete
 */
public class AutocompleteResult {
    
    private final List<SearchHit> hits = new ArrayList<SearchHit>();
    private final SearchConfig config;
    
    /**
     * Create an AutocompleteResult to hold results for the given search.
     * @param searchText The input text for the search.
     * @param resultLimit The limit for number of results returned by the search.
     */
    public AutocompleteResult(SearchConfig config) {
        this.config = config;
    }
    
    /**
     * Add a match result to this result set.
     */
    public void addSearchHit(SearchHit hit) {
        this.hits.add(hit);
    }
    
    /**
     * Add the match results to this result set.
     */
    public void addAllSearchHits(Collection<SearchHit> hits) {
        this.hits.addAll(hits);
    }
    
    /**
     * The match results for this search.
     */
    public List<SearchHit> getResults() {
        return Collections.unmodifiableList(hits);
    }
     
    /**
     * The configuration used to conduct the search.
     */
    public SearchConfig getSearchConfig() {
        return this.config;
    }

}
