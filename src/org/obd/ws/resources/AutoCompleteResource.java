package org.obd.ws.resources;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;
import org.nescent.informatics.OBDQuery;
import org.obd.model.Node;
import org.obd.query.Shard;
import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Reference;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.Variant;

public class AutoCompleteResource extends Resource {

	private final String text;
	private String[] options;
	private JSONObject jObjs;
	private Shard obdsql;

	private Map<String, Set<String>> nameToOntologyMap;
	
	private String synonymOption,definitionOption, ontologies;
	
	private final String ID_STRING = "id";
	private final String NAME_STRING = "name";
	private final String SYNONYM_STRING = "synoynm";
	private final String DEF_STRING = "definition";
	private final String MATCH_TYPE_STRING = "match_type";
	private final String MATCH_TEXT_STRING = "match_text";
	private final String MATCHES_STRING = "matches";
	
	public AutoCompleteResource(Context context, Request request,
			Response response) {
		super(context, request, response);
		this.obdsql = (Shard)this.getContext().getAttributes().get("shard");
		this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));

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
	//	System.out.println(nameOption);
		this.options = new String[]{synonymOption, definitionOption, ontologies};
		
		/**
		 * A hard coded mapping from ontology prefixes for auto completion to the actual default namespaces stored in the database
		 */

	}

	// this constructor is to be used only for testing purposes
	@Deprecated
	public AutoCompleteResource(Shard obdsql, String text, String... options) {
		this.text = text;
		this.obdsql = obdsql;
		this.options = options;
		this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
	}

	public Representation getRepresentation(Variant variant) {

		Representation rep = null;

		try {
			this.jObjs = getTextMatches(this.text.toLowerCase(), this.options);
			if(this.jObjs == null){
				getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST, "ERROR: ZFIN ontology cannot be used in conjunction with other ontologies");
				return null;
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
		rep = new JsonRepresentation(this.jObjs);

		return rep;

	}

	private JSONObject getTextMatches(String text, String... options)
			throws IOException, SQLException, ClassNotFoundException,
			JSONException {

		JSONObject jObj = new JSONObject();
		OBDQuery obdq = new OBDQuery(obdsql);
		String bySynonymOption = ((options[0] == null || options[0].length() == 0) ? "false"
				: options[0]);
		String byDefinitionOption = ((options[1] == null || options[1].length() == 0) ? "false"
				: options[1]);
		List<String> byOntologyOption = null;
		boolean zfinOption = false;
		if(options[2] != null && options[2].length() > 0){
			if (options[2].equals("ZFIN")){
				zfinOption = true;
			}
			else if(options[2].contains("ZFIN")){
				return null;
			}
			else{
				byOntologyOption = new ArrayList<String>();
				for(String choice : options[2].split(",")){
					if(nameToOntologyMap.containsKey(choice)){
						byOntologyOption.addAll(nameToOntologyMap.get(choice));
					}
				}
			}
		}

		Map<String, Collection<Node>> results = obdq.getCompletionsForSearchTerm(text, zfinOption, byOntologyOption,
				new String[] {bySynonymOption, byDefinitionOption});
		
		Collection<Node> nameNodes = results.get("name-matches");
		Collection<Node> synonymNodes = results.get("synonym-matches");
		Collection<Node> definitionNodes = results.get("definition-matches");
		
		Set<JSONObject> matches = new HashSet<JSONObject>();
		
		if(nameNodes.size() > 0){
			for(Node node : nameNodes){
				JSONObject nameMatch = new JSONObject();
				nameMatch.put(ID_STRING, node.getId());
				nameMatch.put(NAME_STRING, node.getLabel());
				nameMatch.put(MATCH_TYPE_STRING, NAME_STRING);
				nameMatch.put(MATCH_TEXT_STRING, node.getLabel());
				matches.add(nameMatch);
//				System.out.println(". Name matches for search term: " + text + "\tID: " + node.getId() + "\tLABEL: " + node.getLabel());
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
	//			System.out.println(++j + ". Synonym matches for search term: " + text + "\tID: " + node.getId() + "\tLABEL: " + 
	//					node.getLabel() + "\tSYNONYM: " + node.getStatements()[0].getTargetId());
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
				//	System.out.println(++k + ". Definition matches for search term: " + text + "\tID: " + node.getId() + "\tLABEL: " + 
				//	node.getLabel() + "\tDefinition: " + node.getStatements()[0].getTargetId());
			}
		}
		jObj.put("search_term", text);
		try{
			List<JSONObject> sortedMatches = sortJsonObjects(matches, text);
			jObj.put(MATCHES_STRING, sortedMatches);
		}
		catch(JSONException e){
			jObj.put(MATCHES_STRING, matches);
		}

		
//		jObj.put("synonymMatches", synonymMatches);
//		jObj.put("definitionMatches", definitionMatches);
		return jObj;
	}

	/**
	 * This method has been created to sort the matches returned by the search string
	 * @param matches
	 * @return
	 */
	private List<JSONObject> sortJsonObjects(Set<JSONObject> matchObjs, String term) throws JSONException{
		List<JSONObject> sortedMatches = new ArrayList<JSONObject>();
		List<JSONObject> labelMatchesStartingWithSearchTerm = new ArrayList<JSONObject>();
		List<JSONObject> otherLabelMatches = new ArrayList<JSONObject>();
		List<JSONObject> synMatchesStartingWithSearchTerm = new ArrayList<JSONObject>();
		List<JSONObject> otherSynMatches = new ArrayList<JSONObject>();
		List<JSONObject> defMatchesStartingWithSearchTerm = new ArrayList<JSONObject>();
		List<JSONObject> otherDefMatches = new ArrayList<JSONObject>();
		
		for(JSONObject matchObj : matchObjs){
			if(matchObj.get(MATCH_TYPE_STRING).equals(DEF_STRING)){
				if(matchObj.getString(MATCH_TEXT_STRING).toLowerCase().startsWith(term.toLowerCase())){
					defMatchesStartingWithSearchTerm.add(matchObj);
				}
				else{
					otherDefMatches.add(matchObj);
				}
			}
			else if(matchObj.get(MATCH_TYPE_STRING).equals(SYNONYM_STRING)){
				if(matchObj.getString(MATCH_TEXT_STRING).toLowerCase().startsWith(term.toLowerCase())){
					synMatchesStartingWithSearchTerm.add(matchObj);
				}
				else{
					otherSynMatches.add(matchObj);
				}
			}
			else{
				if(matchObj.getString(MATCH_TEXT_STRING).toLowerCase().startsWith(term.toLowerCase())){
					labelMatchesStartingWithSearchTerm.add(matchObj);
				}
				else{
					otherLabelMatches.add(matchObj);
				}
			}
		}
		
		sortedMatches.addAll(assort(labelMatchesStartingWithSearchTerm));
		sortedMatches.addAll(assort(synMatchesStartingWithSearchTerm));
		sortedMatches.addAll(assort(defMatchesStartingWithSearchTerm));
		sortedMatches.addAll(assort(otherLabelMatches));
		sortedMatches.addAll(assort(otherSynMatches));
		sortedMatches.addAll(assort(otherDefMatches));
		return sortedMatches;
	}
	
	List<JSONObject> assort(List<JSONObject> inputList) throws JSONException{
		Map<String, JSONObject> nameToObjectMap = new HashMap<String, JSONObject>();
		List<String> names = new ArrayList<String>();
		List<JSONObject> outputList = new ArrayList<JSONObject>();
		for(JSONObject obj : inputList){
			names.add(obj.getString(MATCH_TEXT_STRING));
			nameToObjectMap.put(obj.getString(MATCH_TEXT_STRING), obj);
		}
		Collections.sort(names);
		for(String name : names){
			outputList.add(nameToObjectMap.get(name));
		}
		return outputList;
	}
}
