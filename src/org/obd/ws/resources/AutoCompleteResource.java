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

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.obd.query.Shard;
import org.phenoscape.obd.OBDQuery;
import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Reference;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.ResourceException;
import org.restlet.resource.Variant;

public class AutoCompleteResource extends Resource {

	/** The term to be searched for */
	private String text;
	/** The synonym option to be used in the search */
	private String synonymOption;
	/** The definition option to be used in the search */
	private String definitionOption;
	/** The list of ontologies whose terms are to be matched against */
	private String ontologyOption;	
	/** This indicates search for genes. Needs to be handled separately because these don't come from an ontology */ 
	
	private JSONObject jObjs;
	/** The Shard object that connects to the database */
	private Shard obdsql;
	/** The SQL connection to be used */
	private Logger log;

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
	
	private static final Comparator<JSONObject> MATCHES_COMPARATOR = new Comparator<JSONObject>() {
		public int compare(JSONObject o1, JSONObject o2) {
			try {
				return ((String)o1.get(MATCH_TEXT_STRING)).compareToIgnoreCase((String)o2.get(MATCH_TEXT_STRING));
			} catch (JSONException e) {
				return 0;
			}
		}
	};
	
	/**
	 * @PURPOSE This construnctor instantiates all the instance variables and also
	 * reads in the form parameters. It also does a little work towards seeting up the final query
	 * @param context - the application context
	 * @param request - the Rest request
	 * @param response - the Rest response
	 * @throws SQLException
	 */
	public AutoCompleteResource(Context context, Request request, Response response) throws SQLException {
		super(context, request, response);
		this.obdsql = (Shard)this.getContext().getAttributes().get("shard");
		this.log = Logger.getLogger(this.getClass());
		this.searchOptions = new HashMap<String, String>();
		
		this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
		
        this.text = Reference.decode((String) request.getResourceRef().getQueryAsForm().getFirstValue("text"));
        text = text.trim().toLowerCase();
		if(request.getResourceRef().getQueryAsForm().getFirstValue("syn") != null)
			synonymOption = Reference.decode((String) request.getResourceRef().getQueryAsForm().getFirstValue("syn"));
		if(request.getResourceRef().getQueryAsForm().getFirstValue("def") != null)
			definitionOption = Reference.decode((String) request.getResourceRef().getQueryAsForm().getFirstValue("def"));
		if(request.getResourceRef().getQueryAsForm().getFirstValue("ontology") != null)
			ontologyOption = Reference.decode((String) request.getResourceRef().getQueryAsForm().getFirstValue("ontology"));
		
		final String limitParameter = request.getResourceRef().getQueryAsForm().getFirstValue("limit");
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
	 * @return
	 */
	private boolean inputFormParametersAreValid(){
		if(text == null){
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
				log.error("The value for the limit parameter was not a valid integer", e);
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
    @Override
	public Representation represent(Variant variant) 
            throws ResourceException {

		Representation rep = null;

		try {
			this.jObjs = getTextMatches();
		} catch (JSONException e) {
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, 
					"[JSON EXCEPTION] Something broke on the JSON Object side. Consult server logs");
                    log.error(e);
            return null;
		} catch (SQLException e) {
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, 
				"[SQL EXCEPTION] Something broke on the SQL query. Consult server logs");
            	log.error(e);
            return null;
		}
		
		rep = new JsonRepresentation(this.jObjs);

		return rep;
	}

	/**
	 * This method arranges the user defined options and invokes the OBDQuery method to find
	 * label (default), synonym and definition (optional) matches for the search string. 
	 * @param text - search string
	 * @param options - contains the definition, synonym, and ontology restriction options
	 * @return
	 * @throws IOException
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws JSONException
	 */
	private JSONObject getTextMatches()
            throws JSONException, SQLException {

		JSONObject jObj = new JSONObject();
		OBDQuery obdq = new OBDQuery(obdsql);
		String term = ".*" + text + ".*";
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
				log.debug( ++i + ". Name matches for search term: " + text + "\tID: " + label.get(0) + "\tLABEL: " + label.get(1));
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
				log.debug(++j + ". Synonym matches for search term: " + text + "\tID: " + synonym.get(0) + "\tLABEL: " + 
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
				log.debug(++k + ". Definition matches for search term: " + text + "\tID: " + definition.get(0) + "\tLABEL: " + 
						definition.get(1) + "\tDefinition: " + definition.get(3));
			}
		}
		jObj.put("search_term", text);
		try{
			List<JSONObject> sortedMatches = sortJsonObjects(matches, text);
			jObj.put(MATCHES_STRING, sortedMatches);
		}
		catch(JSONException e){
			log.error("JSON Exception: " + e.getMessage());
			jObj.put(MATCHES_STRING, matches);
                        /* FIXME why are we swallowing this exception? */
		}
		return jObj;
	}

   /**
    * @PURPOSE This method uses the {@link MATCHES_COMPARATOR} to sort
    * the input set of JSON Objects in such a way that:
    * + objects containing labels or synonyms, which start with the search term are
    *   sorted first
    * + objects containing labels or synonyms, which contain the search term are sorted 
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
		List<JSONObject> labelStartsWithMatches = new ArrayList<JSONObject>();
		List<JSONObject> labelContainsMatches = new ArrayList<JSONObject>();
		List<JSONObject> synonymStartsWithMatches = new ArrayList<JSONObject>();
		List<JSONObject> synonymContainsMatches = new ArrayList<JSONObject>();

		List<JSONObject> definitionMatches = new ArrayList<JSONObject>();
		
		for (JSONObject matchObj : matchObjs){
		    if (matchObj.get(MATCH_TYPE_STRING).equals(DEF_STRING)) {
		        definitionMatches.add(matchObj);
		    } else if (matchObj.get(MATCH_TYPE_STRING).equals(SYNONYM_STRING)) {
		        if(matchObj.getString(MATCH_TEXT_STRING).toLowerCase().startsWith(term)){
		            synonymStartsWithMatches.add(matchObj);
		        } else {
		            synonymContainsMatches.add(matchObj);
		        }
		    } else { //actual name matches
		        if (matchObj.getString(MATCH_TEXT_STRING).toLowerCase().startsWith(term)){
		            labelStartsWithMatches.add(matchObj);
		        } else {
		            labelContainsMatches.add(matchObj);
		        }
		    }
		}
		Collections.sort(labelStartsWithMatches, MATCHES_COMPARATOR);
		Collections.sort(labelContainsMatches, MATCHES_COMPARATOR);
		Collections.sort(synonymStartsWithMatches, MATCHES_COMPARATOR);
		Collections.sort(synonymContainsMatches, MATCHES_COMPARATOR);
		Collections.sort(definitionMatches, MATCHES_COMPARATOR);
		sortedMatches.addAll(labelStartsWithMatches);
		sortedMatches.addAll(labelContainsMatches);
		sortedMatches.addAll(synonymStartsWithMatches);
		sortedMatches.addAll(synonymContainsMatches);
		sortedMatches.addAll(definitionMatches);
		if (this.limitOfMatches != null && this.limitOfMatches < sortedMatches.size()) {
		    return sortedMatches.subList(0, this.limitOfMatches);
		} else {
		    return sortedMatches;
		}
	}

}
