package org.phenoscape.obd.query;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Holds information needed to conduct a name search of terms in the Knowledgebase, for example as used for an autocomplete field.
 */
public class SearchConfig {
    
    private final Set<String> namespaces = new HashSet<String>();
    private final String searchText;
    private boolean searchNames = true;
    private boolean searchSynonyms = false;
    private int limit = 0;
    
    /**
     * Create a search config using the provided input text.
     */
    public SearchConfig(String searchText) {
        this.searchText = searchText;
    }
    
    /**
     * The search text used for input.
     */
    public String getSearchText() {
        return this.searchText;
    }
    
    /**
     * Add a namespace ID to search within.
     */
    public void addNamespace(String namespaceID) {
        this.namespaces.add(namespaceID);
    }
    
    
    /**
     * Add namespace IDs to search within.
     */
    public void addAllNamespaces(Collection<String> namespaceIDs) {
        this.namespaces.addAll(namespaceIDs);
    }
    
    /**
     * The namespace, or source, IDs to be searched. At least one namespace should be provided.
     */
    public Set<String> getNamespaces() {
        return Collections.unmodifiableSet(this.namespaces);
    }

    /**
     * Returns whether term names are searched for a match to the input text.
     */
    public boolean searchNames() {
        return searchNames;
    }

    /**
     * Set whether term names are searched for a match to the input text.
     */
    public void setSearchNames(boolean searchNames) {
        this.searchNames = searchNames;
    }

    /**
     * Returns whether term synonyms are searched for a match to the input text.
     */
    public boolean searchSynonyms() {
        return searchSynonyms;
    }

    /**
     * Set whether term synonyms are searched for a match to the input text.
     */
    public void setSearchSynonyms(boolean searchSynonyms) {
        this.searchSynonyms = searchSynonyms;
    }
    
    /**
     * Returns limit on count of match results to return. A limit less than 1 is equivalent to no limit.
     */
    public int getLimit() {
        return this.limit;
    }
    
    /**
     * Set limit on count of match results to return. A limit less than 1 is equivalent to no limit.
     */
    public void setLimit(int limit) {
        this.limit = limit;
    }

}
