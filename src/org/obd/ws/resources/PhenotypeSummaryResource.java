package org.obd.ws.resources;

import java.sql.SQLException;
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
import org.obd.query.Shard;
import org.obd.ws.util.Queries;
import org.obd.ws.util.dto.PhenotypeDTO;
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

public class PhenotypeSummaryResource extends Resource {
	
	protected Logger log = Logger.getLogger(this.getClass());
	
	private String subject_id;
	private String entity_id;
	private String quality_id; 
	private String publication_id;
	private int examples_count = 0;
	
	private Map<String, String> parameters;
	
	private JSONObject jObjs;
	private Shard obdsql;
	private OBDQuery obdq;
	private Queries queries;
	
    /**
     * FIXME Constructor and parameter documentation missing.
     */
public PhenotypeSummaryResource(Context context, Request request, Response response) {
		super(context, request, response);

		this.obdsql = (Shard) this.getContext().getAttributes().get("shard");
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
		if(request.getResourceRef().getQueryAsForm().getFirstValue("examples") != null){
			this.examples_count = Integer.parseInt(Reference.decode((String)(request.getResourceRef().getQueryAsForm().getFirstValue("examples"))));
		}
		
		jObjs = new JSONObject();
		parameters = new HashMap<String, String>();
		queries = new Queries(obdsql);
		 try{
	       	obdq = new OBDQuery(obdsql);
		 }catch(SQLException e){
	      	this.jObjs = null;
	       	getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, 
				"[SQL EXCEPTION] Something broke server side. " +
				"Ontology prefix to node id map of OBDQuery object could not " +
				"be constructed.");
		 }
	}

	/**
	 * The control method for the REST resource. 
	 * All the checks for correct parameter values
	 * are done here before the data processing methods
	 * are invoked 
	 */
	public Representation represent(Variant variant) 
            throws ResourceException {

		Representation rep;
		Map<String, Map<String, List<Set<String>>>> ecAnnots;

		if (subject_id != null && !subject_id.startsWith("TTO:") && !subject_id.contains("GENE")) {
			this.jObjs = null;
			getResponse().setStatus(
					Status.CLIENT_ERROR_BAD_REQUEST,
					"ERROR: The input parameter for subject "
							+ "is not a recognized taxon or gene");
			return null;
		}
		/* Commenting out this section to let post compositions work - Cartik 06/03/09
		if(entity_id != null && !entity_id.startsWith("TAO:") && !entity_id.startsWith("ZFA:")){
			this.jObjs = null;
			getResponse().setStatus(
					Status.CLIENT_ERROR_BAD_REQUEST,
					"ERROR: The input parameter for entity "
							+ "is not a recognized anatomical entity");
			return null;
		}
		*/
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
		try{
			ecAnnots = getAnnotationSummary(subject_id, entity_id, quality_id, publication_id);
		}
		/* 'getAnnotationSummary' method returns null in case of a server side exception*/
		catch(SQLException sqle){
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, 
					"[SQL EXCEPTION] Something broke server side. Consult server logs");
			return null;
		}
		
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
                    /* FIXME Need to provide information to the
                     * client, so add an appropriate message.
                     */
                    log.error(jsone);
                    throw new ResourceException(jsone);
		}
		rep = new JsonRepresentation(this.jObjs);
		return rep;
	}

	/**
	 * This method takes the nodes returned by OBDQuery class and
	 * packages them into a summary data structure. The summary
	 * structure is grouped by quality and subgrouped by
	 * anatomical entity
	 * @param subject_id - can be a TTO taxon or ZFIN GENE
	 * @param entity_id - can only be a TAO term (anatomical entity)
	 * @param char_id - PATO character
	 * @param pub_id - publication id or citation information
	 * @return 
	 * @throws SQLException 
	 */
    /**
     * FIXME Method documentation incomplete.
     */
	private Map<String, Map<String, List<Set<String>>>> 
			getAnnotationSummary(String subject_id, String entity_id, String char_id, String pub_id) 
			throws SQLException{
		
		Map<String, Map<String, List<Set<String>>>> entityCharAnnots = 
			new HashMap<String, Map<String, List<Set<String>>>>(); 
		Map<String, List<Set<String>>> charAnnots;
		List<Set<String>> annots;
		Set<String> gAnnots, tAnnots, qAnnots;
		
		/* This is a data structure to keep track of user specified filter options. 
		 * Four filtering options can be specified viz. entity, character, subject, and 
		 * publication  */
		Map<String, String> filterOptions = new HashMap<String, String>();
		
		String characterId = null, taxonId = null, entityId = null, qualityId = null,
					character = null, taxon = null, entity = null, quality = null;
		String query, searchTerm;
		/* 
		 * This IF-ELSE decides which query to use. Ideally if subject is provided, we will use
		 * gene or taxon query. Otherwise, we use entity query
		 */
		if(subject_id != null){
			if(subject_id.contains("GENE"))
				query = queries.getGeneSummaryQuery();
			else
				query = queries.getTaxonSummaryQuery();
			searchTerm = subject_id;
			filterOptions.put("subject", null);
			filterOptions.put("entity", entity_id);
		}
		else{
			/*	neither subject or entity are provided. so we use the root TAO term
			 * which returns every phenotype in the database
			 */
			query = queries.getAnatomyQuery();
			searchTerm = (entity_id != null ? entity_id : "TAO:0100000");
			filterOptions.put("subject", subject_id);
			filterOptions.put("entity", null);
		}
		filterOptions.put("character", char_id);
		filterOptions.put("publication", null); //TODO pub_id goes here;
		
		log.debug("Search Term: " + searchTerm + " Query: " + query);
		try{
			for(PhenotypeDTO node : obdq.executeQueryAndAssembleResults(query, searchTerm, filterOptions)){

				characterId = node.getCharacterId();
				character = node.getCharacter();
				taxonId = node.getTaxonId();
				taxon = node.getTaxon();
				entityId = node.getEntityId();
				entity = node.getEntity();
				qualityId = node.getQualityId();
				quality = node.getQuality();
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
		}
		catch(SQLException e){
			log.fatal(e);
			throw e;
		}
		return entityCharAnnots;
	}
}
