package org.obd.ws.resources;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.bbop.dataadapter.DataAdapterException;
import org.json.JSONException;
import org.json.JSONObject;
import org.obd.query.Shard;
import org.obd.ws.exceptions.PhenoscapeTreeAssemblyException;
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
	Map<String, String> queryResultsFilterSpecs;
	private JSONObject jObjs;
	private Shard obdsql;
	private OBDQuery obdq;
	
	private Queries queries;
	
	private TTOTaxonomy ttoTaxonomy;
	private TaxonomyBuilder taxonomyBuilder;
    
	/**
	 * This constructor reads in objects from the application context, initializes
	 * instance variables and reads in the form data into the instance variables
	 * @param context
	 * @param request
	 * @param response
	 * @throws IOException
	 * @throws DataAdapterException
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
		queryResultsFilterSpecs = new HashMap<String, String>();
	}
	
   /**
    * This method is responsible for generating the object
    * which is output at the endpoint interface. It calls methods
    * which check the input form data, execute the query and 
    * process the results into a JSON object for delivery to
    * the client 
    * 
    */
	public Representation represent(Variant variant) 
            throws ResourceException {

		Representation rep;
		Map<NodeDTO, List<List<String>>> annots;

		if(!checkInputFormParameters()){
			this.jObjs = null;
			return null;
		}
		
		try{
			annots = getAnnotations();
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
		} catch (PhenoscapeTreeAssemblyException e){
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, 
					"[TREE ASSEMBLY EXCEPTION] Something broke server side. Consult server logs");
			return null;
		}
		try{
			this.jObjs = this.assembleJSONObjectFromDataStructure(annots);
		}
		catch(ResourceException e){
			getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST, 
					"[RESOURCE EXCEPTION] Something broke in the JSON Object. Consult server logs");
			return null;
		}
		rep = new JsonRepresentation(this.jObjs);
		return rep;
	}

	/**
	 * @PURPOSE: This method takes the nodes returned by OBDQuery class and packages them into a 
	 * data structure, which will be processed by the caller {@link represent} method
	 * @return 
	 * @throws SQLException 
	 * @throws IOException 
	 * @throws DataAdapterException 
	 * @throws PhenoscapeTreeAssemblyException 
	 */
	private Map<NodeDTO, List<List<String>>> 
			getAnnotations() 
			throws SQLException, IOException, DataAdapterException, PhenoscapeTreeAssemblyException{
		
		Map<NodeDTO, List<List<String>>> taxonToAssertionsMap = new HashMap<NodeDTO, List<List<String>>>();

		List<String> queryAndSearchTerm = assembleQueryAndSearchTerm();
		String query = queryAndSearchTerm.get(0);
		String searchTerm = queryAndSearchTerm.get(1);
		
		log.trace("Search Term: " + searchTerm + " Query: " + query);
		try{
			Collection<PhenotypeDTO> phenotypeColl = 
				obdq.executeQueryAndAssembleResults(query, searchTerm, queryResultsFilterSpecs);
			if(type != null)
				phenotypeColl = filterCollectionByType(phenotypeColl, type);
			if(type != null && type.equals("evo")){
				taxonToAssertionsMap = 
					generateTreeBasedDataStructureFromAssertions(taxonToAssertionsMap, phenotypeColl);
			}
			else{
				taxonToAssertionsMap = 
					generateSimpleDataStructureFromAssertions(taxonToAssertionsMap, phenotypeColl);
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
		} catch(PhenoscapeTreeAssemblyException e){
			log.fatal(e);
			throw e;
		}
		return taxonToAssertionsMap;
	}
	
	/**
	 * A method to check if the input parameters are
	 * valid
	 * @return - a boolean to indicate validity of input
	 * form parameters
	 */
	private boolean checkInputFormParameters(){
		if (subject_id != null && !subject_id.startsWith("TTO:") && !subject_id.contains("GENE")) {
			
			getResponse().setStatus(
					Status.CLIENT_ERROR_BAD_REQUEST,
					"ERROR: The input parameter for subject "
							+ "is not a recognized taxon or gene");
			return false;
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
			getResponse().setStatus(
					Status.CLIENT_ERROR_BAD_REQUEST,
					"ERROR: The input parameter for quality "
							+ "is not a recognized PATO quality");
			return false;
		}
		if(type != null && !type.equals("evo") && !type.equals("devo")){
			getResponse().setStatus(
					Status.CLIENT_ERROR_BAD_REQUEST,
					"ERROR: [INVALID PARAMETER] The input parameter for taxon type can only be "
							+ "'evo' or 'devo'");
			return false;
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
					return false;
				}
			}
		}
		return true;
	}
	
	/**
	 * This method takes an input data structure and translates it
	 * into a JSON Object
	 * @param annots - the input data structure
	 * @return
	 * @throws ResourceException
	 */
	private JSONObject assembleJSONObjectFromDataStructure
				(Map<NodeDTO, List<List<String>>> annots) 
					throws ResourceException{
		JSONObject subjectObj, qualityObj, entityObj, phenotypeObj; 
		List<JSONObject> phenotypeObjs;
		List<JSONObject> subjectObjs = new ArrayList<JSONObject>();
		JSONObject result = new JSONObject();
		try{
			for(NodeDTO taxonDTO : annots.keySet()){
				subjectObj = new JSONObject();
				subjectObj.put("id", taxonDTO.getId());
				subjectObj.put("name", taxonDTO.getName());
				phenotypeObjs = new ArrayList<JSONObject>();
				for(List<String> phenotype : annots.get(taxonDTO)){
					String count = phenotype.get(4);
					if(count == null)
						count = "";
					entityObj = new JSONObject();
					entityObj.put("id", phenotype.get(0));
					entityObj.put("name", phenotype.get(1));
					qualityObj = new JSONObject();
					qualityObj.put("id", phenotype.get(2));
					qualityObj.put("name", phenotype.get(3));
					phenotypeObj = new JSONObject();
					phenotypeObj.put("entity", entityObj);
					phenotypeObj.put("quality", qualityObj);
					phenotypeObj.put("count", count);
					phenotypeObj.put("id", phenotype.get(5));
					phenotypeObjs.add(phenotypeObj);
				}
				subjectObj.put("phenotypes", phenotypeObjs);
				subjectObjs.add(subjectObj);
			}
			log.trace(annots.size() + " annotations returned");
			result.put("subjects", subjectObjs);
		}
		catch(JSONException jsone){
                    log.error(jsone);
                    throw new ResourceException(jsone);
		}
		return result;
	}
	/**
	 * A helper method which crea6tes a query based upon the 
	 * input parameters and sets up the filter options as well
	 * @return - a two member list comprising the final query and 
	 * the search term for the query
	 */
	private List<String> assembleQueryAndSearchTerm(){
		
		String query, searchTerm;
		if(subject_id != null){
			if(subject_id.contains("GENE"))
				query = queries.getGeneQuery();
			else
				query = queries.getTaxonQuery();
			searchTerm = subject_id;
			queryResultsFilterSpecs.put("entity", entity_id);
		}
		else{
			query = queries.getAnatomyQuery();
			/*	neither subject or entity are provided. so we use the root TAO term
			 * which returns every phenotype in the database
			 */
			searchTerm = (entity_id != null ? entity_id : "TAO:0100000");
		}
		queryResultsFilterSpecs.put("character", quality_id);
		queryResultsFilterSpecs.put("publication", null); //TODO pub_id goes here;
		return Arrays.asList(new String[]{query, searchTerm});
	}
	
	/**
	 * @PURPOSE This method uses an input collection of taxon to 
	 * phenotype assertions to create a data structure, which is 
	 * passed to the calling {@link getAnnotations} method
	 * @param taxonToAssertionsMap - the data structure to be updated
	 * @param phenotypeColl - the input set of taxon to phenotype assertions
	 * @return - the input data structure after updates from the input set of 
	 * taxon to phenotype assertions
	 * @throws IOException
	 * @throws DataAdapterException
	 * @throws PhenoscapeTreeAssemblyException
	 */
	private Map<NodeDTO, List<List<String>>> 
		generateTreeBasedDataStructureFromAssertions(
				Map<NodeDTO, List<List<String>>> taxonToAssertionsMap, 
				Collection<PhenotypeDTO> phenotypeColl) 
				throws IOException, DataAdapterException, PhenoscapeTreeAssemblyException{
		taxonomyBuilder = new TaxonomyBuilder(ttoTaxonomy, phenotypeColl);
		TaxonTree taxonTree = taxonomyBuilder.getTree();
		
		NodeDTO mrca = taxonTree.getMrca();
		if(group.equals("root")){
			List<List<String>> listOfEQCRLists = 
				taxonTree.getNodeToListOfEQCRListsMap().get(mrca);
			taxonToAssertionsMap.put(mrca, listOfEQCRLists);
		}else{
			NodeDTO node = ttoTaxonomy.getIdToNodeMap().get(group);
			Set<NodeDTO> children = taxonTree.getNodeToChildrenMap().get(node);
			if(children != null){
				for(NodeDTO child : children){
					List<List<String>> listOfEQCRLists = 
						taxonTree.getNodeToListOfEQCRListsMap().get(child);
					taxonToAssertionsMap.put(child, listOfEQCRLists);
				}
			}
			else{
				throw new PhenoscapeTreeAssemblyException("");
			}
		}
		return taxonToAssertionsMap;
	}
	
	/**
	 * This method generates a simple Taxon to List of Phenotypes
	 * map from the input taxon to phenotype assertions, which is 
	 * passed to the calling {@link getAnnotations} method
	 * @param taxonToAssertionsMap - the map from taxa to phenotype assertions
	 * @param phenotypeColl - the input collection of taxon to phenotype assertions
	 * @return
	 */
	private Map<NodeDTO, List<List<String>>> 
		generateSimpleDataStructureFromAssertions(
			Map<NodeDTO, List<List<String>>> taxonToAssertionsMap, 
			Collection<PhenotypeDTO> phenotypeColl){
		for(PhenotypeDTO phenotypeDTO : phenotypeColl){
			NodeDTO taxonDTO = new NodeDTO(phenotypeDTO.getTaxonId());
			taxonDTO.setName(phenotypeDTO.getTaxon());
			
			List<List<String>> annotations = taxonToAssertionsMap.get(taxonDTO);
			if(annotations == null)
				annotations = new ArrayList<List<String>>();
			annotations.add(Arrays.asList(new String[]{
					phenotypeDTO.getEntityId(), 
					phenotypeDTO.getEntity(),
					phenotypeDTO.getQualityId(),
					phenotypeDTO.getQuality(),
					phenotypeDTO.getNumericalCount(),
					phenotypeDTO.getReifId()
				}));
			taxonToAssertionsMap.put(taxonDTO, annotations);
		}
		return taxonToAssertionsMap;
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
