package org.obd.ws.resources;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
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
import org.obd.model.Node;
import org.obd.query.Shard;
import org.phenoscape.obd.OBDQuery;
import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Reference;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.ResourceException;
import org.restlet.resource.Variant;

public class AutoCompleteResource extends Resource {

	private final String text;
	private String[] options;
	private JSONObject jObjs;
	private Shard obdsql;
	private Logger log;

	/*
	 * This structure 'nameToOntologyMap' maps commonly used prefixes to the 
	 * default namespaces of the ontologies
	 */
	private Map<String, Set<String>> nameToOntologyMap;
	
    /* FIXME it sounds like a bad idea to hard-code the list of
     * ontologies in a general purpose piece of OBD-WS code
     */
	private String synonymOption,definitionOption, 
				ontologies = "oboInOwl,Relations,PATO,ZFA,ZFIN,Stages,TAO,TTO,Collection,Spatial,Sequence,Units,Phenoscape";
	
	/**
	 * The maximum number of matches to return.
	 */
	private Integer limit = null;
	
	private static final String ID_STRING = "id";
	private static final String NAME_STRING = "name";
	private static final String SYNONYM_STRING = "synonym";
	private static final String DEF_STRING = "definition";
	private static final String MATCH_TYPE_STRING = "match_type";
	private static final String MATCH_TEXT_STRING = "match_text";
	private static final String MATCHES_STRING = "matches";
	
	private static final Comparator<JSONObject> MATCHES_COMPARATOR = new Comparator<JSONObject>() {
	        public int compare(JSONObject o1, JSONObject o2) {
	            try {
                    return ((String)o1.get(MATCH_TEXT_STRING)).compareTo((String)o2.get(MATCH_TEXT_STRING));
                } catch (JSONException e) {
                    return 0;
                }
	        }
	    };
	
    /**
     * FIXME Constructor and parameter documentation missing.
     */
	public AutoCompleteResource(Context context, Request request, Response response) {
		super(context, request, response);
		this.obdsql = (Shard)this.getContext().getAttributes().get("shard");
		
		this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));

                /* FIXME This seems to be a poor way of doing
                 * this. Shouldn't this at a minimum be read in from a
                 * file, and/or use constants are encoded in a
                 * separate class. And can this not dynamically be
                 * obtained from the database? It is also more or less
                 * duplicated from TermResource.
                 */
		/*
		 * A hard coded mapping from ontology prefixes for auto completion to the actual default namespaces stored in the database
		 */
		Set<String> ontologyList;
		nameToOntologyMap = new HashMap<String, Set<String>>();
		ontologyList = new HashSet<String>();
		ontologyList.add("oboInOwl");
		ontologyList.add("oboInOwl:Subset");
		nameToOntologyMap.put("oboInOwl", ontologyList);
		
		ontologyList = new HashSet<String>();
		ontologyList.add("relationship");
		nameToOntologyMap.put("Relations", ontologyList);

		ontologyList = new HashSet<String>();
		ontologyList.add("quality");
		ontologyList.add("pato.ontology");
		nameToOntologyMap.put("PATO", ontologyList);
		
		ontologyList = new HashSet<String>();
		ontologyList.add("zebrafish_anatomy");
		nameToOntologyMap.put("ZFA", ontologyList);
		
		ontologyList = new HashSet<String>();
		ontologyList.add("zebrafish_stages");
		ontologyList.add("zebrafish_anatomical_ontology");
		nameToOntologyMap.put("Stages", ontologyList);

		ontologyList = new HashSet<String>();		
		ontologyList.add("teleost_anatomy");
		nameToOntologyMap.put("TAO", ontologyList);
		
		ontologyList = new HashSet<String>();
		ontologyList.add("teleost-taxonomy");
		nameToOntologyMap.put("TTO",ontologyList);
		
		ontologyList = new HashSet<String>();
		ontologyList.add("museum");
		nameToOntologyMap.put("Collection", ontologyList);
		
		ontologyList = new HashSet<String>();
		ontologyList.add("spatial"); 
		nameToOntologyMap.put("Spatial", ontologyList);
		
		ontologyList = new HashSet<String>();
		ontologyList.add("sequence");
		nameToOntologyMap.put("Sequence", ontologyList);
		
		ontologyList = new HashSet<String>();
		ontologyList.add("unit.ontology");
		nameToOntologyMap.put("Units", ontologyList);
		
		ontologyList = new HashSet<String>();
		ontologyList.add("phenoscape_vocab");
		nameToOntologyMap.put("Phenoscape", ontologyList);
		
        this.text = Reference.decode((String) request.getResourceRef().getQueryAsForm().getFirstValue("text"));
		if(request.getResourceRef().getQueryAsForm().getFirstValue("syn") != null)
			synonymOption = Reference.decode((String) request.getResourceRef().getQueryAsForm().getFirstValue("syn"));
		if(request.getResourceRef().getQueryAsForm().getFirstValue("def") != null)
			definitionOption = Reference.decode((String) request.getResourceRef().getQueryAsForm().getFirstValue("def"));
		if(request.getResourceRef().getQueryAsForm().getFirstValue("ontology") != null)
			ontologies = Reference.decode((String) request.getResourceRef().getQueryAsForm().getFirstValue("ontology"));
		this.options = new String[]{synonymOption, definitionOption, ontologies};
		
		final String limitParameter = request.getResourceRef().getQueryAsForm().getFirstValue("limit");
		if (limitParameter != null) {
		    try {
		    this.limit = Integer.parseInt(limitParameter);
		    if (this.limit < 1) {
		        this.limit = null;
		    }
		    } catch (NumberFormatException e) {
		        log.error("The value for the limit parameter was not a valid integer", e);
		        this.limit = null;
		    }
		}
		
		this.log = Logger.getLogger(this.getClass());

	}

    /**
     * FIXME Constructor and parameter documentation missing.
     */
	// this constructor is to be used only for testing purposes
	AutoCompleteResource(Shard obdsql, String text, String... options) {
		this.text = text;
		this.obdsql = obdsql;
		this.options = options;
		this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
	}

    /**
     * FIXME Method and parameter documentation missing.
     */
        @Override
	public Representation represent(Variant variant) 
            throws ResourceException {

		Representation rep = null;

		try {
			this.jObjs = getTextMatches(this.text.toLowerCase(), this.options);
		} catch (JSONException e) {
                    /* FIXME Need to provide information to the
                     * client, so add an appropriate message.
                     */
                    log.error(e);
                    throw new ResourceException(e);
		} catch (SQLException e) {
                    /* FIXME Need to provide information to the
                     * client, so add an appropriate message.
                     */
                    log.error(e);
                    throw new ResourceException(e);
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
	private JSONObject getTextMatches(String text, String... options)
            throws JSONException, SQLException {

		JSONObject jObj = new JSONObject();
		OBDQuery obdq = new OBDQuery(obdsql);
		String bySynonymOption = ((options[0] == null || options[0].length() == 0) ? "false"
				: options[0]);
		String byDefinitionOption = ((options[1] == null || options[1].length() == 0) ? "false"
				: options[1]);
		List<String> byOntologyOption = null;
		/*
		 * We use a separate option for ZFIN because GENEs do not come from an ontology
		 * but from a text file and are to be queried separately. For terms that come from 
		 * an ontology, we can use source ids in the queries
		 */
		boolean zfinOption = false; 
		if(options[2] != null && options[2].length() > 0){
			if (options[2].contains("ZFIN"))
				zfinOption = true;
			byOntologyOption = new ArrayList<String>();
			for(String choice : options[2].split(",")){
				if(nameToOntologyMap.containsKey(choice)){
					byOntologyOption.addAll(nameToOntologyMap.get(choice));
				}
			}
		}
		
		Map<String, Collection<Node>> results = obdq.getCompletionsForSearchTerm(text, zfinOption, byOntologyOption,
				Boolean.parseBoolean(bySynonymOption), Boolean.parseBoolean(byDefinitionOption));
		
		Collection<Node> nameNodes = results.get(OBDQuery.AutoCompletionMatchTypes.LABEL_MATCH.name());
		Collection<Node> synonymNodes = results.get(OBDQuery.AutoCompletionMatchTypes.SYNONYM_MATCH.name());
		Collection<Node> definitionNodes = results.get(OBDQuery.AutoCompletionMatchTypes.DEFINITION_MATCH.name());
		
		Set<JSONObject> matches = new HashSet<JSONObject>();
		int i = 0, j = 0, k = 0;
		if(nameNodes.size() > 0){
			for(Node node : nameNodes){
				JSONObject nameMatch = new JSONObject();
				nameMatch.put(ID_STRING, node.getId());
				nameMatch.put(NAME_STRING, node.getLabel());
				nameMatch.put(MATCH_TYPE_STRING, NAME_STRING);
				nameMatch.put(MATCH_TEXT_STRING, node.getLabel());
				matches.add(nameMatch);
				log.debug( ++i + ". Name matches for search term: " + text + "\tID: " + node.getId() + "\tLABEL: " + node.getLabel());
			}
		}
		if(synonymNodes != null && synonymNodes.size() > 0){
			for(Node node : synonymNodes){
				JSONObject synonymMatch = new JSONObject();
				synonymMatch.put(ID_STRING, node.getId());
				synonymMatch.put(NAME_STRING, node.getLabel());
				synonymMatch.put(MATCH_TYPE_STRING, SYNONYM_STRING);
				synonymMatch.put(MATCH_TEXT_STRING, node.getStatements()[0].getTargetId());
				matches.add(synonymMatch);
				log.debug(++j + ". Synonym matches for search term: " + text + "\tID: " + node.getId() + "\tLABEL: " + 
						node.getLabel() + "\tSYNONYM: " + node.getStatements()[0].getTargetId());
			}
		}
		if(definitionNodes != null && definitionNodes.size() > 0){
			for(Node node : definitionNodes){
				JSONObject definitionMatch = new JSONObject();
				definitionMatch.put(ID_STRING, node.getId());
				definitionMatch.put(NAME_STRING, node.getLabel());
				definitionMatch.put(MATCH_TYPE_STRING, DEF_STRING);
				definitionMatch.put(MATCH_TEXT_STRING, node.getStatements()[0].getTargetId());
				matches.add(definitionMatch);
				log.debug(++k + ". Definition matches for search term: " + text + "\tID: " + node.getId() + "\tLABEL: " + 
					node.getLabel() + "\tDefinition: " + node.getStatements()[0].getTargetId());
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
     * FIXME Method (what, why, how) and parameter documentation missing.
     */
	/**
	 * This method has been created to sort the matches returned by the search string
	 * Terms starting with the search string are placed higher than terms which only
	 * contain the search string. And labels go first, synonyms next, definitions last
	 * @param matches
	 * @return
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
		if (this.limit != null && this.limit < sortedMatches.size()) {
		    return sortedMatches.subList(0, this.limit);
		} else {
		    return sortedMatches;
		}
	}

}
