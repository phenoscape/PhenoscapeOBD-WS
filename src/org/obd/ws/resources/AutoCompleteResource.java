package org.obd.ws.resources;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
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
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.Variant;

public class AutoCompleteResource extends Resource {

	private final String text;
	private String[] options;
	private JSONObject jObjs;
	private Shard obdsql;

	private String nameOption, synonymOption,definitionOption, ontologies;
	
	public AutoCompleteResource(Context context, Request request,
			Response response) {
		super(context, request, response);
		this.obdsql = (Shard)this.getContext().getAttributes().get("shard");
		this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
		// this.getVariants().add(new Variant(MediaType.TEXT_HTML));
		this.text = Reference.decode((String) (request.getAttributes()
				.get("text")));
		if(request.getAttributes().get("name") != null)
			nameOption = Reference.decode((String) request.getAttributes().get("name"));
		if(request.getAttributes().get("syn") != null)
			synonymOption = Reference.decode((String) request.getAttributes().get("syn"));
		if(request.getAttributes().get("def") != null)
			definitionOption = Reference.decode((String) request.getAttributes().get("def"));
		if(request.getAttributes().get("ontology") != null)
			ontologies = Reference.decode((String) request.getAttributes().get("ontology"));
	//	System.out.println(nameOption);
		this.options = new String[]{nameOption, synonymOption, definitionOption, ontologies};

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
		String stringRep = "";

		try {
			this.jObjs = getTextMatches(this.text, this.options);
			stringRep = this.renderJsonObjectAsString(this.jObjs, 0);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		rep = new JsonRepresentation(this.jObjs);
		System.out.println(stringRep);

		return rep;

	}

	private JSONObject getTextMatches(String text, String... options)
			throws IOException, SQLException, ClassNotFoundException,
			JSONException {

		JSONObject jObj = new JSONObject();
		OBDQuery obdq = new OBDQuery(obdsql);
		String byNameOption = options[0];
		String bySynonymOption = ((options[1] == null || options[0].length() == 0) ? "false"
				: options[1]);
		String byDefinitionOption = ((options[2] == null || options[2].length() == 0) ? "false"
				: options[2]);
		String byOntologyOption = (options[3] == null || options[3].length() == 0) ? "none"
				: options[3];

		/*if (!Boolean.parseBoolean(byNameOption)) {
			throw new IllegalArgumentException(
					"Search by Name parameter is set to false");
		}*/
		Map<String, Collection<Node>> results = obdq.getCompletionsForSearchTerm(text,
				new String[] { bySynonymOption, byDefinitionOption, byOntologyOption });
		
		Collection<Node> nameNodes = results.get("name-matches");
		Collection<Node> synonymNodes = results.get("synonym-matches");
		Collection<Node> definitionNodes = results.get("definition-matches");
		
		Set<JSONObject> matches = new HashSet<JSONObject>();
		
		if(nameNodes.size() > 0){
			for(Node node : nameNodes){
				JSONObject nameMatch = new JSONObject();
				nameMatch.put("id", node.getId());
				nameMatch.put("name", text);
				nameMatch.put("match_type", "name");
				nameMatch.put("match_text", node.getLabel());
				matches.add(nameMatch);
//				System.out.println(". Name matches for search term: " + text + "\tID: " + node.getId() + "\tLABEL: " + node.getLabel());
			}
		}
		if(synonymNodes != null && synonymNodes.size() > 0){
			for(Node node : synonymNodes){
				JSONObject synonymMatch = new JSONObject();
				synonymMatch.put("id", node.getId());
				synonymMatch.put("name", text);
				synonymMatch.put("match_type", "synonym");
				synonymMatch.put("match_text", node.getStatements()[0].getTargetId());
				matches.add(synonymMatch);
	//			System.out.println(++j + ". Synonym matches for search term: " + text + "\tID: " + node.getId() + "\tLABEL: " + 
	//					node.getLabel() + "\tSYNONYM: " + node.getStatements()[0].getTargetId());
			}
		}
		if(definitionNodes != null && definitionNodes.size() > 0){
			for(Node node : definitionNodes){
				JSONObject definitionMatch = new JSONObject();
				definitionMatch.put("id", node.getId());
				definitionMatch.put("name", text);
				definitionMatch.put("match_type", "definition");
				definitionMatch.put("match_text", node.getStatements()[0].getTargetId());
				matches.add(definitionMatch);
				//	System.out.println(++k + ". Definition matches for search term: " + text + "\tID: " + node.getId() + "\tLABEL: " + 
				//	node.getLabel() + "\tDefinition: " + node.getStatements()[0].getTargetId());
			}
		}
		jObj.put("matches", matches);
//		jObj.put("synonymMatches", synonymMatches);
//		jObj.put("definitionMatches", definitionMatches);
		return jObj;
	}


	private String renderJsonObjectAsString(JSONObject jo, int indentCt) throws JSONException {
		String output = "";
		String tabs = "";
		for(int ct = 0; ct < indentCt; ct++){
			tabs += "\t";
		}
		output += "{\n";
		
		String idPart, namePart, relationPart, defPart;
		if (jo.has("id") && jo.get("id") != null) {
			relationPart = "id: " + (String) jo.get("id") + "\n";
			output += tabs + relationPart;
		}
		if (jo.has("name") && jo.get("name") != null) {
			idPart = "name: " + (String) jo.get("name") + "\n";
			output += tabs + idPart;
		}
		if (jo.has("match_type") && jo.get("match_type") != null) {
			namePart = "match_type: " + (String) jo.get("match_type") + "\n";
			output += tabs + namePart;
		}
		if (jo.has("match_text") && jo.get("match_text") != null) {
			defPart = "match_text: " + (String) jo.get("match_text") + "\n";
			output += tabs + defPart;
		}

		if (jo.has("matches")) {
			JSONArray matches = (JSONArray) jo.get("matches");
			if (matches != null) {
				output += "matches: \n[\n";
				for(int i = 0; i < matches.length(); i++){
					JSONObject parent = matches.getJSONObject(i);
					output += renderJsonObjectAsString(parent, indentCt);
				}
			}
			output += "]\n";
		}
		/*
		if (jo.has("synonymMatches")) {
			JSONArray synonymMatches = (JSONArray) jo.get("synonymMatches");
			if (synonymMatches != null) {
	//			output += "children: \n";
				for(int j = 0; j < synonymMatches.length(); j++){
					JSONObject child = synonymMatches.getJSONObject(j);
					output += renderJsonObjectAsString(child, indentCt);
				}
			}
		}
		if (jo.has("definitionMatches")) {
			JSONArray definitionMatches = (JSONArray) jo.get("definitionMatches");
			if (definitionMatches != null) {
	//			output += "links: \n";
				for(int k = 0; k < definitionMatches.length(); k++){
					JSONObject other = definitionMatches.getJSONObject(k);
					output += renderJsonObjectAsString(other, indentCt);
				}
			}
		}
		*/
		output += tabs + "}\n";
		return output;
	}

}
