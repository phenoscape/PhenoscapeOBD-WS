package org.obd.ws.resources;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.nescent.informatics.OBDQuery;
import org.obd.model.Node;
import org.obd.model.Statement;
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

public class PhenotypeDetailsResource extends Resource {

	Logger log = Logger.getLogger(this.getClass());
	
	private String subject_id;
	private String entity_id;
	private String quality_id; 
	private String publication_id;
	
	private Map<String, String> parameters;
	
	private JSONObject jObjs;
	private Shard obdsql;
	private Connection conn;
	private OBDQuery obdq;
	
	public PhenotypeDetailsResource(Context context, Request request, Response response) {
		super(context, request, response);

		this.obdsql = (Shard) this.getContext().getAttributes().get("shard");
		this.conn = (Connection)this.getContext().getAttributes().get("conn");
		this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));

		if(request.getResourceRef().getQueryAsForm().getFirstValue("subject") != null){
			this.subject_id = Reference.decode((String) (request.getResourceRef().getQueryAsForm().getFirstValue("subject")));
		}
		if(request.getResourceRef().getQueryAsForm().getFirstValue("entity") != null){
			this.entity_id = Reference.decode((String)(request.getResourceRef().getQueryAsForm().getFirstValue("entity")));
		}
		if(request.getResourceRef().getQueryAsForm().getFirstValue("quality") != null){
			this.quality_id = Reference.decode((String)(request.getResourceRef().getQueryAsForm().getFirstValue("quality")));
		}
		if(request.getResourceRef().getQueryAsForm().getFirstValue("publication") != null){
			this.publication_id = Reference.decode((String)(request.getResourceRef().getQueryAsForm().getFirstValue("publication")));
		}
		
		String aq = (String)this.getContext().getAttributes().get("anatomyQuery");
		String tq = (String)this.getContext().getAttributes().get("taxonQuery");
		String gq = (String)this.getContext().getAttributes().get("geneQuery");
		
		obdq = new OBDQuery(obdsql, conn, new String[]{aq, tq, gq});
		jObjs = new JSONObject();
		parameters = new HashMap<String, String>();
	}
	
	public Representation getRepresentation(Variant variant) {

		Representation rep;

		if (subject_id != null && !subject_id.startsWith("TTO:") && !subject_id.contains("GENE:")) {
			this.jObjs = null;
			getResponse().setStatus(
					Status.CLIENT_ERROR_BAD_REQUEST,
					"ERROR: The input parameter for subject "
							+ "is not a recognized taxon or gene");
			return null;
		}
		if(entity_id != null && !entity_id.startsWith("TAO:") && !entity_id.startsWith("ZFA:")){
			this.jObjs = null;
			getResponse().setStatus(
					Status.CLIENT_ERROR_BAD_REQUEST,
					"ERROR: The input parameter for entity "
							+ "is not a recognized anatomical entity");
			return null;
		}
		if(quality_id != null && !quality_id.startsWith("PATO:")){
			this.jObjs = null;
			getResponse().setStatus(
					Status.CLIENT_ERROR_BAD_REQUEST,
					"ERROR: The input parameter for quality "
							+ "is not a recognized PATO quality");
			return null;
		}
			
		//TODO Publication ID check
		
		parameters.put("entity_id", entity_id);
		parameters.put("quality_id", quality_id);
		parameters.put("subject_id", subject_id);
		parameters.put("publication_id", publication_id);
		
		for(String key : parameters.keySet()){
			if(parameters.get(key) != null){
				if(obdsql.getNode(parameters.get(key)) == null){
					getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND,
					"No annotations were found with the specified search parameters");
					return null;
				}
			}
		}
		
		List<List<String[]>> annots = 
			getAnnotations(subject_id, entity_id, quality_id, publication_id);
		List<String[]> comp;
		
		JSONObject subjectObj, qualityObj, entityObj, phenotypeObj; 
		List<JSONObject> phenotypeObjs = new ArrayList<JSONObject>();
		
		try{
			for(int i = 0; i < annots.size(); i++){
				comp = annots.get(i);
				phenotypeObj = new JSONObject();
				subjectObj = new JSONObject();
				entityObj = new JSONObject();
				qualityObj = new JSONObject();
				subjectObj.put("id", comp.get(0)[0]);
				subjectObj.put("name", comp.get(0)[1]);
				entityObj.put("id", comp.get(1)[0]);
				entityObj.put("name", comp.get(1)[1]);
				qualityObj.put("id", comp.get(2)[0]);
				qualityObj.put("name", comp.get(2)[1]);
				phenotypeObj.put("subject", subjectObj);
				phenotypeObj.put("entity", entityObj);
				phenotypeObj.put("quality", qualityObj);
				phenotypeObjs.add(phenotypeObj);
			}
			log.trace(annots.size() + " annotations returned");
			this.jObjs.put("phenotypes", phenotypeObjs);
		}
		catch(JSONException jsone){
			jsone.printStackTrace();
		}
		rep = new JsonRepresentation(this.jObjs);
		return rep;
	}

	private List<List<String[]>> 
			getAnnotations(String subject_id, String entity_id, String char_id, String pub_id){
		
		Map<String, String> nodeProps;
				
		List<List<String[]>> results = new ArrayList<List<String[]>>();
		List<String[]> annots;
		
		String[] filterOptions = new String[4];
		
		String relId, target, characterId = null, taxonId = null, entityId = null, qualityId = null,
					character = null, taxon = null, entity = null, quality = null;
		String query, searchTerm;
		
		query = obdq.getAnatomyQuery();
		
		searchTerm = (entity_id != null ? entity_id : "TAO:0100000");
		filterOptions[1] = null;
		filterOptions[0] = subject_id;
		filterOptions[2] = char_id;
		filterOptions[3] = null; //TODO pub_id goes here; 
		
		log.trace("Search Term: " + searchTerm + " Query: " + query);
		for(Node node : obdq.executeQuery(query, searchTerm, filterOptions)){
			nodeProps = new HashMap<String, String>();
			for(Statement stmt : node.getStatements()){
				relId = stmt.getRelationId();
				target = stmt.getTargetId();
				nodeProps.put(relId, target);
			} 
			nodeProps.put("id", node.getId());
			characterId = nodeProps.get("hasCharacterId");
			character = nodeProps.get("hasCharacter");
			taxonId = nodeProps.get("exhibitedById");
			taxon = nodeProps.get("exhibitedBy");
			entityId = nodeProps.get("inheresInId");
			entity = nodeProps.get("inheresIn");
			qualityId = nodeProps.get("hasStateId");
			quality = nodeProps.get("hasState");
			log.trace("Char: " + characterId + " [" + character + "] Taxon: " + taxonId + "[" + taxon + "] Entity: " +
					entityId + "[" + entity + "] Quality: " + qualityId + "[" + quality + "]");
			annots = new ArrayList<String[]>();
			annots.add(new String[]{taxonId, taxon});
			annots.add(new String[]{entityId, entity});
			annots.add(new String[]{qualityId, quality});
			results.add(annots);
		}
		return results;
	}
}
