package org.phenoscape.obd.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AutocompleteResult {
    
    private final List<SearchHit> hits = new ArrayList<SearchHit>();
    private final String searchText;
    
    public AutocompleteResult(String searchText) {
        this.searchText = searchText;
    }
    
    public List<SearchHit> getResults() {
        return Collections.unmodifiableList(hits);
    }
    
    public int getResultLimit() {
        //TODO
        return 0;
    }
    
    public int getCompleteResultCount() {
        //TODO
        return 0;
    }
    
    public String getSearchText() {
        return this.searchText;
    }

}
