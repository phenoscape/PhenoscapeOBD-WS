package org.phenoscape.ws.resource;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.model.Vocab.GO;
import org.phenoscape.obd.model.Vocab.PATO;
import org.phenoscape.obd.model.Vocab.PHENOSCAPE;
import org.phenoscape.obd.model.Vocab.TAO;
import org.phenoscape.obd.model.Vocab.TTO;
import org.phenoscape.obd.model.Vocab.ZFIN;
import org.phenoscape.obd.query.AutocompleteResult;
import org.phenoscape.obd.query.SearchConfig;
import org.phenoscape.obd.query.SearchHit;
import org.phenoscape.obd.query.SearchHit.MatchType;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public class AutocompleteResource extends AbstractPhenoscapeResource {

    private boolean matchName = true;
    private boolean matchSynonym = false;
    private String searchText = null;
    private int limit = 0;
    private final Set<String> searchNamespaces = new HashSet<String>();

    private static final Map<String, String[]> prefixes = new HashMap<String, String[]>();
    static {
        prefixes.put("tao", new String[] {TAO.NAMESPACE});
        prefixes.put("go", new String[] {GO.NAMESPACE, GO.BP_NAMESPACE, GO.CC_NAMESPACE, GO.MF_NAMESPACE});
        prefixes.put("pato", new String[] {PATO.NAMESPACE});
        prefixes.put("tto", new String[] {TTO.NAMESPACE});
        prefixes.put("zfin", new String[] {ZFIN.GENE_NAMESPACE});
        prefixes.put("pspub", new String[] {PHENOSCAPE.PUB_NAMESPACE});
    }
    
    private static final Map<String, String[]> termTypes = new HashMap<String, String[]>();
    static {
        termTypes.put("entity", new String[] {TAO.NAMESPACE, GO.NAMESPACE, GO.BP_NAMESPACE, GO.CC_NAMESPACE, GO.MF_NAMESPACE});
        termTypes.put("quality", new String[] {PATO.NAMESPACE});
        termTypes.put("taxon", new String[] {TTO.NAMESPACE});
        termTypes.put("gene", new String[] {ZFIN.GENE_NAMESPACE});
        termTypes.put("pub", new String[] {PHENOSCAPE.PUB_NAMESPACE});
    }

    private static final Map<MatchType, String> matchTypes = new HashMap<MatchType, String>();
    static {
        matchTypes.put(MatchType.NAME, "name");
        matchTypes.put(MatchType.SYNONYM, "syn");
    }

    @Override
    protected void doInit() throws ResourceException {
        super.doInit();
        this.searchText = this.getFirstQueryValue("text");
        this.matchName = this.getBooleanQueryValue("name", this.matchName);
        this.matchSynonym = this.getBooleanQueryValue("syn", this.matchSynonym);
        this.limit = this.getIntegerQueryValue("limit", this.limit);
        final String prefixChoices = this.getFirstQueryValue("ontology");
        if (prefixChoices != null) { this.parsePrefixChoices(prefixChoices); }
        final String termTypeChoices = this.getFirstQueryValue("type");
        if (termTypeChoices != null) { this.parseTermTypeChoices(termTypeChoices); }
    }

    @Get("json")
    public Representation getJSONRepresentation() {
        try {
            if (this.searchText == null) {
                this.setStatus(Status.CLIENT_ERROR_BAD_REQUEST, "A search term must be provided.");
                return null;
            } else {
                return new JsonRepresentation(this.translate(this.queryForMatches()));   
            }
        } catch (JSONException e) {
            log().error("Failed to create JSON object", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        } catch (SQLException e) {
            log().error("Database error querying for autocomplete", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        }
    }
    
    private void parsePrefixChoices(String prefixChoices) {
        for (String item : prefixChoices.split(",")) {
            final String text = item.trim().toLowerCase();
            if (prefixes.containsKey(text)) {
                this.searchNamespaces.addAll(Arrays.asList(prefixes.get(text)));
            }
        }
    }
    
    private void parseTermTypeChoices(String termTypeChoices) {
        for (String item : termTypeChoices.split(",")) {
            final String text = item.trim().toLowerCase();
            if (termTypes.containsKey(text)) {
                this.searchNamespaces.addAll(Arrays.asList(termTypes.get(text)));
            }
        }
    }

    private JSONObject translate(AutocompleteResult result) throws JSONException, SQLException {
        final JSONObject json = new JSONObject();
        final List<JSONObject> matches = new ArrayList<JSONObject>();
        for (SearchHit hit : result.getResults()) {
            matches.add(this.translate(hit));
        }
        json.put("matches", matches);
        json.put("search_term", result.getSearchConfig().getSearchText());
        return json;
    }

    private JSONObject translate(SearchHit hit) throws JSONException {
        final JSONObject json = new JSONObject();
        json.put("id", hit.getHit().getUID());
        json.put("name", hit.getHit().getLabel());
        json.put("match_type", matchTypes.get(hit.getMatchType()));
        json.put("match_text", hit.getMatchText());
        json.put("source", TermResourceUtil.translateMinimal(hit.getHit().getSource()));
        return json;
    }

    private AutocompleteResult queryForMatches() throws SQLException {
        final SearchConfig config = new SearchConfig(this.searchText);
        config.setSearchNames(this.matchName);
        config.setSearchSynonyms(this.matchSynonym);
        config.setLimit(this.limit);
        if (this.searchNamespaces.isEmpty()) {
            config.addAllNamespaces(this.flatten(prefixes.values()));
            config.addAllNamespaces(this.flatten(termTypes.values()));
        } else {
            config.addAllNamespaces(this.searchNamespaces);
        }
        return this.getDataStore().getAutocompleteMatches(config);
    }
    
    private Collection<String> flatten(Collection<String[]> items) {
        final List<String> allItems = new ArrayList<String>();
        for (String[] itemArray : items) {
            allItems.addAll(Arrays.asList(itemArray));
        }
        return allItems;
    }    

}
