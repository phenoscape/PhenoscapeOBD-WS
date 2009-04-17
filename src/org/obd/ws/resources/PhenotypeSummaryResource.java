package org.obd.ws.resources;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

public class PhenotypeSummaryResource extends Resource {
	
	Logger log = Logger.getLogger(this.getClass());
	
	private String subject_id;
	private String entity_id;
	private String quality_id; 
	private String publication_id;
	private int examples_count = 2;
	
	private Map<String, String> parameters;
	
	private JSONObject jObjs;
	private Shard obdsql;
	private Connection conn;
	private OBDQuery obdq;
	
	
	public PhenotypeSummaryResource(Context context, Request request, Response response) {
		super(context, request, response);

		this.obdsql = (Shard) this.getContext().getAttributes().get("shard");
		this.conn = (Connection)this.getContext().getAttributes().get("conn");
		this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
		// this.getVariants().add(new Variant(MediaType.TEXT_HTML));
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
		if(request.getResourceRef().getQueryAsForm().getFirstValue("examples") != null){
			this.examples_count = Integer.parseInt(Reference.decode((String)(request.getResourceRef().getQueryAsForm().getFirstValue("examples"))));
		}
		String aq = (String)this.getContext().getAttributes().get("anatomyQuery");
		String tq = (String)this.getContext().getAttributes().get("taxonQuery");
		String gq = (String)this.getContext().getAttributes().get("geneQuery");
		
		obdq = new OBDQuery(obdsql, conn, new String[]{aq, tq, gq});
		jObjs = new JSONObject();
		parameters = new HashMap<String, String>();
		// System.out.println(termId);
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
		
		Map<String, Map<String, List<Set<String>>>> ecAnnots = 
			getAnnotationSummary(subject_id, entity_id, quality_id, publication_id);
		
		JSONObject genesObj, taxaObj, qualitiesObj, exampleObj; 
		JSONObject ecObject, eObject, cObject;
		List<JSONObject> gExampleObjs, tExampleObjs, qExampleObjs;
		List<JSONObject> ecObjects = new ArrayList<JSONObject>();
		
		try{
			for(String entityId : ecAnnots.keySet()){
				Map<String, List<Set<String>>> cAnnots = ecAnnots.get(entityId); 
				for(String charId : cAnnots.keySet()){
					ecObject = new JSONObject();
					eObject = new JSONObject();
					cObject = new JSONObject();
					String[] eComps = entityId.split("\\t");
					String [] cComps = charId.split("\\t");
					eObject.put("id", eComps[0]);
					eObject.put("name", eComps[1]);
					cObject.put("id", cComps[0]);
					cObject.put("name", cComps[1]);
					ecObject.put("entity", eObject);
					ecObject.put("character_quality", cObject);
					
					genesObj= new JSONObject();
					taxaObj = new JSONObject();
					qualitiesObj = new JSONObject();
					Set<String> genesSet = cAnnots.get(charId).get(0);
					Set<String> taxaSet = cAnnots.get(charId).get(1);
					Set<String> qualitiesSet = cAnnots.get(charId).get(2);
					gExampleObjs = new ArrayList<JSONObject>();
					tExampleObjs = new ArrayList<JSONObject>();
					qExampleObjs = new ArrayList<JSONObject>();
					genesObj.put("count", genesSet.size());
					Iterator<String> git = genesSet.iterator();
					for(int i = 0; i < (Math.min(examples_count, genesSet.size())); i++){
						String gene = git.next();
						String[] gComps = gene.split("\\t");
						exampleObj = new JSONObject();
						exampleObj.put("id", gComps[0]);
						exampleObj.put("name", gComps[1]);
						gExampleObjs.add(exampleObj);
					}
					genesObj.put("examples", gExampleObjs);
					taxaObj.put("count", taxaSet.size());
					Iterator<String> tit = taxaSet.iterator();
					for(int i = 0; i < (Math.min(examples_count, taxaSet.size())); i++){
						String taxon = tit.next();
						String[] tComps = taxon.split("\\t");
						exampleObj = new JSONObject();
						exampleObj.put("id", tComps[0]);
						exampleObj.put("name", tComps[1]);
						tExampleObjs.add(exampleObj);
					}
					taxaObj.put("examples", tExampleObjs);
					qualitiesObj.put("count", qualitiesSet.size());
					Iterator<String> qit = qualitiesSet.iterator();
					for(int i = 0; i < (Math.min(examples_count, qualitiesSet.size())); i++){
						String quality = qit.next();
						String[] qComps = quality.split("\\t");
						exampleObj = new JSONObject();
						exampleObj.put("id", qComps[0]);
						exampleObj.put("name", qComps[1]);
						qExampleObjs.add(exampleObj);
					}
					qualitiesObj.put("examples", qExampleObjs);
					
					ecObject.put("qualities", qualitiesObj);
					ecObject.put("taxa", taxaObj);
					ecObject.put("genes", genesObj);
					ecObjects.add(ecObject);
				}
			}
			this.jObjs.put("characters", ecObjects);
		}
		catch(JSONException jsone){
			jsone.printStackTrace();
		}
		rep = new JsonRepresentation(this.jObjs);
		return rep;
	}

	private Map<String, Map<String, List<Set<String>>>> 
			getAnnotationSummary(String subject_id, String entity_id, String char_id, String pub_id){
		
		Map<String, String> nodeProps;
				
		Map<String, Map<String, List<Set<String>>>> entityCharAnnots = 
			new HashMap<String, Map<String, List<Set<String>>>>(); 
		Map<String, List<Set<String>>> charAnnots;
		List<Set<String>> annots;
		Set<String> gAnnots, tAnnots, qAnnots;
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
		
		log.debug("Search Term: " + searchTerm + " Query: " + query);
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
			if(entityCharAnnots.keySet().contains(entityId + "\t" + entity)){
				charAnnots = entityCharAnnots.get(entityId + "\t" + entity);
				
				if(charAnnots.keySet().contains(characterId + "\t" + character)){
					annots = charAnnots.get(characterId + "\t" + character);
					gAnnots = annots.get(0);
					tAnnots = annots.get(1);
					qAnnots = annots.get(2);
				}
				else{
					annots = new ArrayList<Set<String>>();
					gAnnots = new HashSet<String>();
					tAnnots = new HashSet<String>();
					qAnnots = new HashSet<String>();
				}
				qAnnots.add(qualityId + "\t" + quality);
				if(taxonId.contains("GENE")){
					gAnnots.add(taxonId + "\t" + taxon);
				}
				else{
					tAnnots.add(taxonId + "\t" + taxon);
				}
				annots.add(0, gAnnots);
				annots.add(1, tAnnots);
				annots.add(2, qAnnots);
				charAnnots.put(characterId + "\t" + character, annots);
			}
			else{
				charAnnots = new HashMap<String, List<Set<String>>>();
				annots = new ArrayList<Set<String>>();
				gAnnots = new HashSet<String>();
				tAnnots = new HashSet<String>();
				qAnnots = new HashSet<String>();
				qAnnots.add(qualityId + "\t" + quality);
				if(taxonId.contains("GENE")){
					gAnnots.add(taxonId + "\t" + taxon);
				}
				else{
					tAnnots.add(taxonId + "\t" + taxon);
				}
				annots.add(0, gAnnots);
				annots.add(1, tAnnots);
				annots.add(2, qAnnots);
				charAnnots.put(characterId + "\t" + character, annots);
			}
			entityCharAnnots.put(entityId + "\t" + entity, charAnnots);
		}
		return entityCharAnnots;
	}
}
