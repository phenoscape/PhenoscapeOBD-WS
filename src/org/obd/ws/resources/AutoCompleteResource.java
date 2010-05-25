package org.obd.ws.resources;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.query.OBDQuery;
import org.phenoscape.obd.query.Queries;
import org.phenoscape.ws.application.PhenoscapeWebServiceApplication;
import org.restlet.data.Reference;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public class AutoCompleteResource extends AbstractOBDResource {

    private String searchTerm;
    private String synonymOption;
    private String definitionOption;
    /** The list of ontologies whose terms are to be searched */
    private String ontologyOption;	

    private JSONObject jObjs;
    private Queries queries;
    /** The map from default namespaces of the ontologies to their corr. node ids in the database */
    private Map<String, String> nsToSourceIdMap;
    /** The map from prefixes of the terms to the default namespaces of the ontologies they come from */
    private Map<String, Set<String>> prefixToNSMap;


    /** A parameter to limit the number of matches */
    private Integer limitOfMatches = null;

    /** A structure to keep track of the search options input
     * from the form such as ontologyList, synonyms, definitions
     * etc.
     */
    private Map<String, String> searchOptions;

    private static final String ID_STRING = "id";
    private static final String NAME_STRING = "name";
    private static final String SYNONYM_STRING = "synonym";
    private static final String DEF_STRING = "definition";
    private static final String MATCH_TYPE_STRING = "match_type";
    private static final String MATCH_TEXT_STRING = "match_text";
    private static final String MATCHES_STRING = "matches";

    private static final String ONTOLOGY_OPTION_STRING = "ontologyOption";
    private static final String SYNONYM_OPTION_STRING = "synonymOption";
    private static final String DEFINITION_OPTION_STRING = "definitionOption";

    private static final String INPUT_TEXT_STRING = "text";
    private static final String INPUT_SYN_STRING = "syn";
    private static final String INPUT_DEF_STRING = "def";
    private static final String INPUT_ONTOLOGY_STRING = "ontology";
    private static final String INPUT_LIMIT_STRING = "limit";

    /**
     * This comparator is used to order collections of JSON objects. 
     * The property values for the {@literal #MATCH_TEXT_STRING} is used 
     * to compare the JSON objects 
     */
    private static final Comparator<JSONObject> MATCHES_COMPARATOR = new Comparator<JSONObject>() {
        public int compare(JSONObject o1, JSONObject o2) {
            try {
                return ((String)o1.get(MATCH_TEXT_STRING)).compareToIgnoreCase((String)o2.get(MATCH_TEXT_STRING));
            } catch (JSONException e) {
                return 0;
            }
        }
    };

    @SuppressWarnings("unchecked")
    @Override
    protected void doInit() throws ResourceException {
        super.doInit();
        this.queries = (Queries)this.getContext().getAttributes().get(PhenoscapeWebServiceApplication.QUERIES_STRING);
        this.prefixToNSMap = (Map<String, Set<String>>)this.getContext().getAttributes().get(PhenoscapeWebServiceApplication.PREFIX_TO_DEFAULT_NAMESPACE_MAP_STRING);
        this.nsToSourceIdMap = (Map<String, String>)this.getContext().getAttributes().get(PhenoscapeWebServiceApplication.DEFAULT_NAMESPACE_TO_SOURCE_ID_MAP_STRING);
        this.searchOptions = new HashMap<String, String>();

        this.searchTerm = Reference.decode((String) this.getQuery().getFirstValue(INPUT_TEXT_STRING));
        searchTerm = searchTerm.trim().toLowerCase();
        if(this.getQuery().getFirstValue(INPUT_SYN_STRING) != null)
            synonymOption = Reference.decode((String) this.getQuery().getFirstValue(INPUT_SYN_STRING));
        if(this.getQuery().getFirstValue(INPUT_DEF_STRING ) != null)
            definitionOption = Reference.decode((String) this.getQuery().getFirstValue(INPUT_DEF_STRING ));
        if(this.getQuery().getFirstValue(INPUT_ONTOLOGY_STRING) != null)
            ontologyOption = Reference.decode((String) this.getQuery().getFirstValue(INPUT_ONTOLOGY_STRING));

        final String limitParameter = this.getQuery().getFirstValue(INPUT_LIMIT_STRING);
        if (limitParameter != null)
            this.limitOfMatches = Integer.parseInt(limitParameter);
        if(!inputFormParametersAreValid()){
            throw new IllegalArgumentException("Invalid form parameters");
        }
        searchOptions.put(SYNONYM_OPTION_STRING, synonymOption);
        searchOptions.put(DEFINITION_OPTION_STRING, definitionOption);
        searchOptions.put(ONTOLOGY_OPTION_STRING, ontologyOption);
    }



    /**
     * A method to check if input parameters from the form are valid
     * @return a boolean to indicate if the input form parameters are valid
     */
    private boolean inputFormParametersAreValid(){
        if(searchTerm == null){
            getResponse().setStatus(
                    Status.CLIENT_ERROR_BAD_REQUEST,
            "ERROR: Please specify a string to search for");
            return false;
        }
        if (limitOfMatches != null) {
            try {
                if (this.limitOfMatches < 1) {
                    this.limitOfMatches = null;
                }
            } catch (NumberFormatException e) {
                log().error("The value for the limit parameter was not a valid integer", e);
                this.limitOfMatches = null;
                return false;
            }
        }
        return true;
    }

    /**
     * This method is responsible for creating the representation of the JSON Object
     * from the query results which will be forwarded to the invoking client 
     */
    @Get("json")
    public Representation getJSONRepresentation() throws ResourceException {

        Representation rep = null;
        try {
            connectShardToDatabase();
            this.jObjs = getTextMatches();
        } catch (JSONException e) {
            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, 
            "[JSON EXCEPTION] Something broke on the JSON Object side. Consult server logs");
            log().error(e);
            return null;
        } catch (SQLException e) {
            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, 
            "[SQL EXCEPTION] Something broke on the SQL query. Consult server logs");
            log().error(e);
            return null;
        } catch (ClassNotFoundException e) {
            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, 
            "[CLASS NOT FOUND EXCEPTION] Something broke on the SQL query. Consult server logs");
            log().error(e);
            return null;
        } finally{
            disconnectShardFromDatabase();
        }
        rep = new JsonRepresentation(this.jObjs);
        return rep;
    }

    /**
     * This method arranges the user defined options and invokes the 
     * {@link OBDQuery#getAutocompletionsForSearchTerm(String, Map)}  method to find
     * label (default), synonym and definition (optional) matches for the search string. 
     * @return JSON object to be sent to the UI
     * @throws IOException
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws JSONException
     */
    private JSONObject getTextMatches() throws JSONException, SQLException {

        JSONObject jObj = new JSONObject();
        OBDQuery obdq = new OBDQuery(this.getShard(), this.queries, this.nsToSourceIdMap, this.prefixToNSMap);
        //For search terms shorter than 3, only "begins with" searches are done  
        String term = searchTerm.length() > 3 ? "%" + searchTerm + "%" : searchTerm + "%";
        Map<String, List<List<String>>> results = 
            obdq.getAutocompletionsForSearchTerm(term, searchOptions);

        List<List<String>> labelMatches = 
            results.get(OBDQuery.AutoCompletionMatchTypes.LABEL_MATCH.name());
        List<List<String>> synonymMatches = 
            results.get(OBDQuery.AutoCompletionMatchTypes.SYNONYM_MATCH.name());
        List<List<String>> definitionMatches = 
            results.get(OBDQuery.AutoCompletionMatchTypes.DEFINITION_MATCH.name());

        Set<JSONObject> matches = new HashSet<JSONObject>();
        int i = 0, j = 0, k = 0;
        if(labelMatches.size() > 0){
            for(List<String> label : labelMatches){
                JSONObject nameMatch = new JSONObject();
                nameMatch.put(ID_STRING, label.get(0));
                nameMatch.put(NAME_STRING, label.get(1));
                nameMatch.put(MATCH_TYPE_STRING, NAME_STRING);
                nameMatch.put(MATCH_TEXT_STRING, label.get(1));
                matches.add(nameMatch);
                log().debug( ++i + ". Name matches for search term: " + searchTerm + "\tID: " + label.get(0) + "\tLABEL: " + label.get(1));
            }
        }
        if(synonymMatches != null && synonymMatches.size() > 0){
            for(List<String> synonym : synonymMatches){
                JSONObject synonymMatch = new JSONObject();
                synonymMatch.put(ID_STRING, synonym.get(0));
                synonymMatch.put(NAME_STRING, synonym.get(1));
                synonymMatch.put(MATCH_TYPE_STRING, SYNONYM_STRING);
                synonymMatch.put(MATCH_TEXT_STRING, synonym.get(2));
                matches.add(synonymMatch);
                log().debug(++j + ". Synonym matches for search term: " + searchTerm + "\tID: " + synonym.get(0) + "\tLABEL: " + 
                        synonym.get(1) + "\tSYNONYM: " + synonym.get(2));
            }
        }
        if(definitionMatches != null && definitionMatches.size() > 0){
            for(List<String> definition : definitionMatches){
                JSONObject definitionMatch = new JSONObject();
                definitionMatch.put(ID_STRING, definition.get(0));
                definitionMatch.put(NAME_STRING, definition.get(1));
                definitionMatch.put(MATCH_TYPE_STRING, DEF_STRING);
                definitionMatch.put(MATCH_TEXT_STRING, definition.get(3));
                matches.add(definitionMatch);
                log().debug(++k + ". Definition matches for search term: " + searchTerm + "\tID: " + definition.get(0) + "\tLABEL: " + 
                        definition.get(1) + "\tDefinition: " + definition.get(3));
            }
        }
        jObj.put("search_term", searchTerm);
        try{
            List<JSONObject> sortedMatches = sortJsonObjects(matches, searchTerm);
            jObj.put(MATCHES_STRING, sortedMatches);
        }
        catch(JSONException e){
            log().error("JSON Exception: " + e.getMessage());
            jObj.put(MATCHES_STRING, matches);
            /* FIXME why are we swallowing this exception? */
        }
        return jObj;
    }

    /**
     * PURPOSE This method uses the {@link #MATCHES_COMPARATOR} to sort
     * the input set of JSON Objects in such a way that:
     * + objects containing labels and synonyms, which start with the search term are
     *   sorted first
     * + objects containing labels and synonyms, which contain the search term are sorted 
     *   second
     * + objects containing definitions containing the search term are sorted 
     *   third 
     * @param matchObjs - the set of JSON objects to be sorted
     * @param term - the search term, which is to be used in the sorting
     * process
     * @return - properly sorted list of JSON Objects
     * @throws JSONException
     */
    private List<JSONObject> sortJsonObjects(Set<JSONObject> matchObjs, String term) throws JSONException{
        List<JSONObject> sortedMatches = new ArrayList<JSONObject>();
        List<JSONObject> startsWithMatches = new ArrayList<JSONObject>();
        List<JSONObject> containedInMatches = new ArrayList<JSONObject>();
        List<JSONObject> definitionMatches = new ArrayList<JSONObject>();

        for (JSONObject matchObj : matchObjs){
            if (matchObj.get(MATCH_TYPE_STRING).equals(DEF_STRING)) {
                definitionMatches.add(matchObj);
            } else if (matchObj.get(MATCH_TYPE_STRING).equals(SYNONYM_STRING)) {
                if(matchObj.getString(MATCH_TEXT_STRING).toLowerCase().startsWith(term.toLowerCase())){
                    startsWithMatches.add(matchObj);
                } else {
                    containedInMatches.add(matchObj);
                }
            } else { //actual name matches
                if (matchObj.getString(MATCH_TEXT_STRING).toLowerCase().startsWith(term.toLowerCase())){
                    startsWithMatches.add(matchObj);
                } else {
                    containedInMatches.add(matchObj);
                }
            }
        }
        Collections.sort(startsWithMatches, MATCHES_COMPARATOR);
        Collections.sort(containedInMatches, MATCHES_COMPARATOR);
        Collections.sort(definitionMatches, MATCHES_COMPARATOR);
        sortedMatches.addAll(startsWithMatches);
        sortedMatches.addAll(containedInMatches);
        sortedMatches.addAll(definitionMatches);
        if (this.limitOfMatches != null && this.limitOfMatches < sortedMatches.size()) {
            return sortedMatches.subList(0, this.limitOfMatches);
        } else {
            return sortedMatches;
        }
    }
}
