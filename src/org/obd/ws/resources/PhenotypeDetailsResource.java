package org.obd.ws.resources;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.bbop.dataadapter.DataAdapterException;
import org.json.JSONException;
import org.json.JSONObject;
import org.obd.query.Shard;
import org.obd.ws.util.Queries;
import org.obd.ws.util.TTOTaxonomy;
import org.obd.ws.util.TaxonTree;
import org.obd.ws.util.TaxonomyBuilder;
import org.obd.ws.util.dto.NodeDTO;
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

public class PhenotypeDetailsResource extends Resource {

	Logger log = Logger.getLogger(this.getClass());
	
	private String subject_id;
	private String entity_id;
	private String quality_id; 
	private String publication_id;
	private String type;
	private String group = "root";
	
	private Map<String, String> parameters;
	
	private JSONObject jObjs;
	private Shard obdsql;
	private OBDQuery obdq;
	
	private Queries queries;
	
	private TTOTaxonomy ttoTaxonomy;
	private TaxonomyBuilder taxonomyBuilder;
    /**
     * FIXME Constructor and parameter documentation missing.
     * @throws DataAdapterException 
     * @throws IOException 
     */
	public PhenotypeDetailsResource(Context context, Request request, Response response) throws IOException, DataAdapterException {
		super(context, request, response);

		this.obdsql = (Shard) this.getContext().getAttributes().get("shard");
		this.ttoTaxonomy = (TTOTaxonomy)this.getContext().getAttributes().get("ttoTaxonomy");
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
		if(request.getResourceRef().getQueryAsForm().getFirstValue("type") != null){
			this.type = Reference.decode((String)(request.getResourceRef().getQueryAsForm().getFirstValue("type")));
		}
		if(request.getResourceRef().getQueryAsForm().getFirstValue("group") != null){
			this.group = Reference.decode((String)(request.getResourceRef().getQueryAsForm().getFirstValue("group")));
		}

		
		queries = new Queries(obdsql);
		obdq = new OBDQuery(obdsql);
		jObjs = new JSONObject();
		parameters = new HashMap<String, String>();
	}
	
    /**
     * FIXME Method and parameter documentation missing.
     */
	public Representation represent(Variant variant) 
            throws ResourceException {

		Representation rep;
		List<List<String[]>> annots;

		if (subject_id != null && !subject_id.startsWith("TTO:") && !subject_id.contains("GENE")) {
			this.jObjs = null;
			getResponse().setStatus(
					Status.CLIENT_ERROR_BAD_REQUEST,
					"ERROR: The input parameter for subject "
							+ "is not a recognized taxon or gene");
			return null;
		}
		/*  Commenting out this section to let post compositions work - Cartik 06/03/09
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
		if(type != null && !type.equals("evo") && !type.equals("devo")){
			this.jObjs = null;
			getResponse().setStatus(
					Status.CLIENT_ERROR_BAD_REQUEST,
					"ERROR: [INVALID PARAMETER] The input parameter for taxon type can only be "
							+ "'evo' or 'devo'");
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
			annots = getAnnotations(subject_id, entity_id, quality_id, publication_id, type);
		}
		/* 'getAnnotations' method returns null in case of a server side exception*/
		catch(SQLException sqle){
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, 
					"[SQL EXCEPTION] Something broke server side. Consult server logs");
			return null;
		} catch (IOException e) {
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, 
					"[IO EXCEPTION] Something broke server side. Consult server logs");
			return null;
		} catch (DataAdapterException e) {
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, 
					"[DATA ADAPTER EXCEPTION] Something broke server side. Consult server logs");
			return null;
		}
		List<String[]> comp;
		Set<String> reifSet;
		JSONObject subjectObj, qualityObj, entityObj, phenotypeObj, reifObj; 
		List<JSONObject> phenotypeObjs = new ArrayList<JSONObject>();
		
		try{
			for(int i = 0; i < annots.size(); i++){
				//TODO add measurements and units info
				comp = annots.get(i);
				reifSet = new HashSet<String>();
				phenotypeObj = new JSONObject();
				subjectObj = new JSONObject();
				entityObj = new JSONObject();
				qualityObj = new JSONObject();
				reifObj = new JSONObject();
				String reifs = comp.get(3)[0];
				for(String reif : reifs.split(",")){
					reifSet.add(reif);
				}
				subjectObj.put("id", comp.get(0)[0]);
				subjectObj.put("name", comp.get(0)[1]);
				entityObj.put("id", comp.get(1)[0]);
				entityObj.put("name", comp.get(1)[1]);
				qualityObj.put("id", comp.get(2)[0]);
				qualityObj.put("name", comp.get(2)[1]);
				reifObj.put("reif_id", comp.get(3)[0]);
				phenotypeObj.put("subject", subjectObj);
				phenotypeObj.put("entity", entityObj);
				phenotypeObj.put("quality", qualityObj);
				phenotypeObj.put("reified_link", reifSet);	
				phenotypeObj.put("count", comp.get(4)[0]);
				phenotypeObjs.add(phenotypeObj);
			}
			log.trace(annots.size() + " annotations returned");
			this.jObjs.put("phenotypes", phenotypeObjs);
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
	 * @PURPOSE: This method takes the nodes returned by OBDQuery class and packages them into a 
	 * data structure
	 * @param subject_id - can be a TTO taxon or ZFIN GENE
	 * @param entity_id - can only be a TAO term (anatomical entity)
	 * @param char_id - PATO character
	 * @param pub_id - publication id or citation information
	 * @param type - can take one of two values. 'evo' for taxon data or 'devo' for ZFIN data
	 * @return 
	 * @throws SQLException 
	 * @throws IOException 
	 * @throws DataAdapterException 
	 */
	private List<List<String[]>> 
			getAnnotations(String subject_id, String entity_id, String char_id, String pub_id, String type) 
			throws SQLException, IOException, DataAdapterException{
		
		List<List<String[]>> results = new ArrayList<List<String[]>>();
		List<String[]> annots;
		
		/* This is a data structure to keep track of user specified filter options. 
		 * Four filtering options can be specified viz. entity, character, subject, and 
		 * publication  */
		Map<String, String> filterOptions = new HashMap<String, String>();
		
		String characterId = null, taxonId = null, entityId = null, qualityId = null,
					character = null, taxon = null, entity = null, quality = null, 
					reifId = null, count = null;
		String query, searchTerm;
		/* 
		 * This IF-THEN decides which query to use. Ideally if subject is provided, we will use
		 * gene or taxon query. Otherwise, we use entity query
		 */
		if(subject_id != null){
			if(subject_id.contains("GENE"))
				query = queries.getGeneQuery();
			else
				query = queries.getTaxonQuery();
			searchTerm = subject_id;
			filterOptions.put("subject", null);
			filterOptions.put("entity", entity_id);
		}
		else{
			query = queries.getAnatomyQuery();
			/*	neither subject or entity are provided. so we use the root TAO term
			 * which returns every phenotype in the database
			 */
			searchTerm = (entity_id != null ? entity_id : "TAO:0100000");
			filterOptions.put("subject", subject_id);
			filterOptions.put("entity", null);
		}
		filterOptions.put("character", char_id);
		filterOptions.put("publication", null); //TODO pub_id goes here;
		
		log.trace("Search Term: " + searchTerm + " Query: " + query);
		try{
			Collection<PhenotypeDTO> phenotypeColl = 
				obdq.executeQueryAndAssembleResults(query, searchTerm, filterOptions);
			if(type != null)
				phenotypeColl = filterCollectionByType(phenotypeColl, type);
			if(type != null && type.equals("evo")){
				taxonomyBuilder = new TaxonomyBuilder(ttoTaxonomy, phenotypeColl);
				NodeDTO mrca = taxonomyBuilder.findMRCA(taxonomyBuilder.getTaxonColl(), null);
				TaxonTree tree = taxonomyBuilder.constructTreeFromPhenotypeDTOs();
				if(group.equals("root")){
					taxonId = mrca.getId();
					taxon = mrca.getName();
					List<List<NodeDTO>> listOfEQCRLists = 
						tree.getNodeToListOfEQCRListsMap().get(mrca);
				}
				
			}
			else{
				for(PhenotypeDTO node : phenotypeColl){
					characterId = node.getCharacterId();
					character = node.getCharacter();
					taxonId = node.getTaxonId();
					taxon = node.getTaxon();
					entityId = node.getEntityId();
					entity = node.getEntity();
					qualityId = node.getQualityId();
					quality = node.getQuality();
					reifId = node.getReifId();
					count = node.getNumericalCount();
					//TODO measurement and unti values to be added here
					log.trace("Char: " + characterId + " [" + character + "] Taxon: " + taxonId + "[" + taxon + "] Entity: " +
							entityId + "[" + entity + "] Quality: " + qualityId + "[" + quality + "]");
					annots = new ArrayList<String[]>();
					annots.add(new String[]{taxonId, taxon});
					annots.add(new String[]{entityId, entity});
					annots.add(new String[]{qualityId, quality});
					annots.add(new String[]{reifId});
					annots.add(new String[]{count});
					//TODO Measurements and units need to be added here
					results.add(annots);
				}
			}
			
		}
		catch(SQLException e){
			log.fatal(e);
			throw e;
		} catch (IOException e) {
			log.fatal(e);
			throw e;
		} catch (DataAdapterException e) {
			log.fatal(e);
			throw e;
		}
		return results;
	}
	
	/**
	 * A method to filter the collection based upon 
	 * the value of the "type" parameter
	 * @param phenotypeColl
	 * @param type - can be "evo" or "devo". This is set in the calling method
	 * @return input collection minus TTOs or GENEs depending on the value of the 
	 * "type" parameter
	 */
	private Collection<PhenotypeDTO> filterCollectionByType(Collection<PhenotypeDTO> phenotypeColl, String type){
		if(type.equals("evo"))
			return filterGenesFromCollection(phenotypeColl);
		else
			return filterTaxaFromCollection(phenotypeColl);
	}

	/**
	 * A simple method that weeds out DTOs with TTOs as their taxa
	 * from the collection
	 * @param phenotypeColl
	 * @return input collection minus TTOs
	 */
	private Collection<PhenotypeDTO> filterTaxaFromCollection(
			Collection<PhenotypeDTO> phenotypeColl) {
		Collection<PhenotypeDTO> collectionToReturn = new ArrayList<PhenotypeDTO>();
		for(PhenotypeDTO phenotypeDTO : phenotypeColl){
			if(phenotypeDTO.getTaxonId().contains("GENE")){
				collectionToReturn.add(phenotypeDTO);
			}
		}
		return collectionToReturn;
	}

	/**
	 * A simple method that weeds out DTOs with GENEs as their taxa
	 * from the collection
	 * @param phenotypeColl
	 * @return input collection minus GENEs
	 */
	private Collection<PhenotypeDTO> filterGenesFromCollection(
			Collection<PhenotypeDTO> phenotypeColl) {
		Collection<PhenotypeDTO> collectionToReturn = new ArrayList<PhenotypeDTO>();
		for(PhenotypeDTO phenotypeDTO : phenotypeColl){
			if(phenotypeDTO.getTaxonId().contains("TTO")){
				collectionToReturn.add(phenotypeDTO);
			}
		}
		return collectionToReturn;
	}
}
