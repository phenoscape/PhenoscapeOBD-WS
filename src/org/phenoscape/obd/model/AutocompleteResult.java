package org.phenoscape.obd.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class AutocompleteResult {
    
    private final List<SearchHit> hits = new ArrayList<SearchHit>();
    private String searchText;
    private int resultLimit;
    private int completeResultCount;
    
    public void addSearchHit(SearchHit hit) {
        this.hits.add(hit);
    }
    
    public void addAllSearchHits(Collection<SearchHit> hits) {
        this.hits.addAll(hits);
    }
    
    public List<SearchHit> getResults() {
        return Collections.unmodifiableList(hits);
    }
        
    public int getResultLimit() {
        return this.resultLimit;
    }
    
    public void setResultLimit(int limit) {
        this.resultLimit = limit;
    }
    
    public int getCompleteResultCount() {
        return this.completeResultCount;
    }
    
    public void setCompleteResultCount(int count) {
        this.completeResultCount = count;
    }
    
    public String getSearchText() {
        return this.searchText;
    }
    
    public void setSearchText(String text) {
        this.searchText = text;
    }

}
