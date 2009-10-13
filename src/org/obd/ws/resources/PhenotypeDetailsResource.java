package org.obd.ws.resources;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.obd.query.impl.OBDSQLShard;
import org.obd.ws.application.OBDApplication;
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
	private final String driverName = "jdbc:postgresql://"; 
	
	private String subject_id;
	private String entity_id;
	private String character_id; 
	private String publication_id;
	private String type;
	private String group;
	
	private Map<String, String> parameters;
	Map<String, String> queryResultsFilterSpecs;
	private JSONObject jObjs;
	private OBDSQLShard obdsqlShard;
	private OBDQuery obdq;
	
	private Queries queries;
	
	private TTOTaxonomy ttoTaxonomy;
	private TaxonomyBuilder taxonomyBuilder;
	private TaxonTree taxonTree;
    
	private static final String SUBJECT_STRING = "subject";
	private static final String ENTITY_STRING = "entity";
	private static final String QUALITY_STRING = "quality";
	private static final String PUBLICATION_STRING = "publication";
	private static final String TYPE_STRING = "type";
	private static final String GROUP_STRING = "group";
	
	/**
	 * This constructor reads in objects from the application context, initializes
	 * instance variables and reads in the form data into the instance variables
	 * @param context
	 * @param request
	 * @param response
	 * @throws IOException
	 * @throws DataAdapterException
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 */
	public PhenotypeDetailsResource(Context context, Request request, Response response) throws IOException, DataAdapterException, 
								SQLException, ClassNotFoundException {
		super(context, request, response);

		this.obdsqlShard = new OBDSQLShard();
		this.ttoTaxonomy = (TTOTaxonomy)this.getContext().getAttributes().get(OBDApplication.TTO_TAXONOMY_STRING);
		this.queries = (Queries)this.getContext().getAttributes().get(OBDApplication.QUERIES_STRING);
		this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));

		if(request.getResourceRef().getQueryAsForm().getFirstValue(SUBJECT_STRING) != null){
			this.subject_id = Reference.decode((String) (request.getResourceRef().getQueryAsForm().getFirstValue(SUBJECT_STRING)));
		}
		if(request.getResourceRef().getQueryAsForm().getFirstValue(ENTITY_STRING) != null){
			this.entity_id = Reference.decode((String)(request.getResourceRef().getQueryAsForm().getFirstValue(ENTITY_STRING)));
		}
		if(request.getResourceRef().getQueryAsForm().getFirstValue(QUALITY_STRING) != null){
			this.character_id = Reference.decode((String)(request.getResourceRef().getQueryAsForm().getFirstValue(QUALITY_STRING)));
		}
		if(request.getResourceRef().getQueryAsForm().getFirstValue(PUBLICATION_STRING) != null){
			this.publication_id = Reference.decode((String)(request.getResourceRef().getQueryAsForm().getFirstValue(PUBLICATION_STRING)));
		}
		if(request.getResourceRef().getQueryAsForm().getFirstValue(TYPE_STRING) != null){
			this.type = Reference.decode((String)(request.getResourceRef().getQueryAsForm().getFirstValue(TYPE_STRING)));
		}
		if(request.getResourceRef().getQueryAsForm().getFirstValue(GROUP_STRING) != null){
			this.group = Reference.decode((String)(request.getResourceRef().getQueryAsForm().getFirstValue(GROUP_STRING)));
		}

		jObjs = new JSONObject();
		parameters = new HashMap<String, String>();
		queryResultsFilterSpecs = new HashMap<String, String>();
	}
	
   /**
    * This method is responsible for generating the object
    * which is output at the endpoint interface. It calls methods
    * which check the input form data, execute the query and 
    * process the results into a JSON object for delivery to
    * the endpoint interface 
    * 
    */
	public Representation represent(Variant variant) 
            throws ResourceException {

		Representation rep;
		Map<NodeDTO, List<List<String>>> annots;

		try{
			this.connectShardToDatabase();
			if(!inputFormParametersAreValid()){
				this.jObjs = null;
				this.disconnectShardFromDatabase();
				return null;
			}
			obdq = new OBDQuery(obdsqlShard, queries);
			annots = getAnnotations();
			this.jObjs = this.assembleJSONObjectFromDataStructure(annots);
		} catch(SQLException sqle){
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
		} catch (ClassNotFoundException e) {
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, 
			"CLASS NOT FOUND EXCEPTION] Something broke server side. Consult server logs");
			return null;
		} catch(ResourceException e){
			getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST, 
					"[RESOURCE EXCEPTION] Something broke in the JSON Object. Consult server logs");
			return null;
		}
		finally{
			this.disconnectShardFromDatabase();
		}
		rep = new JsonRepresentation(this.jObjs);
		return rep;
	}

	/**
     * This method reads in db connection parameters from app context and connects the Shard to the
     * database
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    private void connectShardToDatabase() throws SQLException, ClassNotFoundException{
    	String dbName = (String)this.getContext().getAttributes().get(OBDApplication.SELECTED_DATABASE_NAME_STRING);
    	String dbHost = (String)this.getContext().getAttributes().get(OBDApplication.DB_HOST_NAME_STRING);
    	String uid = (String)this.getContext().getAttributes().get(OBDApplication.UID_STRING);
    	String pwd = (String)this.getContext().getAttributes().get(OBDApplication.PWD_STRING);
    	
    	String dbConnString = driverName + dbHost + "/" + dbName;
    	long connStartTime = System.currentTimeMillis();
    	obdsqlShard.connect(dbConnString, uid, pwd);
    	long connEndTime = System.currentTimeMillis();
    	log().trace("It took " + (connEndTime - connStartTime) + " msecs to connect");
    }
    
    private void disconnectShardFromDatabase(){
    	if(obdsqlShard != null)
    		obdsqlShard.disconnect();
    	obdsqlShard = null;
    }
	
	/**
	 * PURPOSE: This method takes the nodes returned by OBDQuery class and packages them into a 
	 * data structure, which will be processed by the caller {@link #represent()} method
	 * @return a data structure containing the results of the query execution
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
		
		log().trace("Search Term: " + searchTerm + " Query: " + query);
		try{
			Collection<PhenotypeDTO> phenotypeColl = 
				obdq.executeQueryAndAssembleResults(query, searchTerm, queryResultsFilterSpecs);
			if(type != null)
				phenotypeColl = filterCollectionByType(phenotypeColl, type);
			if(type != null && type.equals("evo") && group != null){
				taxonToAssertionsMap = 
					generateTreeBasedDataStructureFromAssertions(taxonToAssertionsMap, phenotypeColl);
			}
			else{
				taxonToAssertionsMap = 
					generateSimpleDataStructureFromAssertions(taxonToAssertionsMap, phenotypeColl);
			}
		} catch(SQLException e){
			log().fatal(e);
			throw e;
		} catch (IOException e) {
			log().fatal(e);
			throw e;
		} catch (DataAdapterException e) {
			log().fatal(e);
			throw e;
		} catch(PhenoscapeTreeAssemblyException e){
			log().fatal(e);
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
	private boolean inputFormParametersAreValid(){
		if (subject_id != null && !subject_id.startsWith("TTO:") && !subject_id.contains("GENE")) {
			
			getResponse().setStatus(
					Status.CLIENT_ERROR_BAD_REQUEST,
					"ERROR: The input parameter for subject "
							+ "is not a recognized taxon or gene");
			return false;
		}
		if(character_id != null && !character_id.startsWith("PATO:")){
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
		if(group != null && !group.equals("root") && !group.matches("TTO:[0-9]+")){
			getResponse().setStatus(
					Status.CLIENT_ERROR_BAD_REQUEST,
					"ERROR: [INVALID PARAMETER] The input parameter for group can only be "
							+ "'root' or a valid TTO taxon");
			return false;
		}
		
		//TODO Publication ID check
		
		parameters.put("entity_id", entity_id);
		parameters.put("quality_id", character_id);
		parameters.put("subject_id", subject_id);
		parameters.put("publication_id", publication_id);
		
		for(String key : parameters.keySet()){
			if(parameters.get(key) != null){
				if(obdsqlShard.getNode(parameters.get(key)) == null){
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
	 * @param annots - data structure containing results of the phenotype
	 * details query. This data structure is obtained from the {@link #getAnnotations()}
	 * method 
	 * @return a JSON object which is returned to the invoking {@link #represent(Variant)} method 
	 * @throws ResourceException
	 */
	private JSONObject assembleJSONObjectFromDataStructure
				(Map<NodeDTO, List<List<String>>> annots) 
					throws ResourceException{
		JSONObject subjectObj, qualityObj, entityObj, phenotypeObj, rankObj, relatedEntityObj; 
		List<JSONObject> phenotypeObjs;
		List<JSONObject> subjectObjs = new ArrayList<JSONObject>();
		Set<String> reifIdSet;
		JSONObject result = new JSONObject();
		try{
			for(NodeDTO taxonDTO : annots.keySet()){
				subjectObj = new JSONObject();
				subjectObj.put("id", taxonDTO.getId());
				subjectObj.put("name", taxonDTO.getName());
				if(taxonDTO.getId().startsWith("TTO")){
					if(ttoTaxonomy.getSetOfExtinctTaxa().contains(taxonDTO))
						subjectObj.put("extinct", true);
					else
						subjectObj.put("extinct", false);
					rankObj = new JSONObject();
					NodeDTO rankDTO = ttoTaxonomy.getTaxonToRankMap().get(taxonDTO);
					if(rankDTO != null){
						rankObj.put("id", rankDTO.getId());
						rankObj.put("name", rankDTO.getName());
					}
					else{
						rankObj.put("id", "");
						rankObj.put("name", "");
					}
					subjectObj.put("rank", rankObj);
					Integer subsumedTaxaCount = 
						(taxonTree != null && taxonTree.getNodeToSubsumedLeafNodesMap().get(taxonDTO) != null)?
						taxonTree.getNodeToSubsumedLeafNodesMap().get(taxonDTO).size() :
						0;
					if(annots.get(taxonDTO).get(0).get(5).length() > 0){
						subjectObj.put("leaf", true);
						subjectObj.put("annotated_taxa_count", subsumedTaxaCount + 1);
					}
					else{
						subjectObj.put("leaf", false);
						subjectObj.put("annotated_taxa_count", subsumedTaxaCount);
					}
				}
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
					reifIdSet = processReifIdsForTaxon(phenotype.get(5));
					if(reifIdSet.size() > 0)
						phenotypeObj.put("id", reifIdSet);
					else
						phenotypeObj.put("id", "");
					relatedEntityObj = new JSONObject();
					relatedEntityObj.put("id", phenotype.get(6));
					relatedEntityObj.put("name", phenotype.get(7));
					phenotypeObj.put("related_entity", relatedEntityObj);
					phenotypeObjs.add(phenotypeObj);
				}
				subjectObj.put("phenotypes", phenotypeObjs);
				subjectObjs.add(subjectObj);
			}
			log().trace(annots.size() + " annotations returned");
			result.put("subjects", subjectObjs);
		}
		catch(JSONException jsone){
                    log().error(jsone);
                    throw new ResourceException(jsone);
		}
		return result;
	}

	/**
	 * A helper method to handle reif ids coming through for the taxa. It 
	 * parses a comma delimited list of reif ids and puts them into a set
	 * @param oneOrMoreReifIds
	 * @return consolidated reif ids in a comma delimited set
	 */
	private Set<String> processReifIdsForTaxon(String oneOrMoreReifIds){
		Set<String> reifIdSet = new HashSet<String>();
		if(oneOrMoreReifIds != null && oneOrMoreReifIds.length() > 0){
			if(oneOrMoreReifIds.contains(",")){
				for(String reifId : oneOrMoreReifIds.split(",")){
					if(reifId.length() > 0)
						reifIdSet.add(reifId);
				}
			}
			else
				reifIdSet.add(oneOrMoreReifIds);
		}
		return reifIdSet;
	}
	/**
	 * A helper method which crea6tes a query based upon the 
	 * input parameters and sets up the filter options as well
	 * @return - a two member list comprising the final query and 
	 * the search term for the query
	 */
	private List<String> assembleQueryAndSearchTerm(){
		
		String query, searchTerm;
		if(subject_id != null && entity_id != null){
			query = queries.getGenericPhenotypeQuery();
			if(subject_id.contains("GENE"))
				query += " AND p1.subject_nid = (SELECT node_id FROM node WHERE uid = '" + subject_id + "')";
			else {
				query += " AND taxon_node.uid = '" + subject_id + "'";
				if(character_id != null)
					query += " AND p1.character_uid = '" + character_id + "'";
				query += " UNION ";
				query += queries.getGenericPhenotypeQueryForSpecificTaxon();
				query += " AND p1.subject_uid = '" + subject_id + "'";
			}
			searchTerm = entity_id; 
		}
		else if(subject_id != null){
			if(subject_id.contains("GENE"))
				query = queries.getGeneQuery();
			else
				query = queries.getTaxonQuery();
			searchTerm = subject_id;
			if(entity_id != null){
				query += " AND entity_uid = '" + entity_id + "'";
			}
		}
		else{
			query = queries.getAnatomyQuery();
			/*	neither subject or entity are provided. so we use the root TAO term
			 * which returns every phenotype in the database
			 */
			searchTerm = (entity_id != null ? entity_id : "TAO:0100000");
		}
		if(character_id != null){
			query += " AND character_uid = '" + character_id + "'";
		}
		queryResultsFilterSpecs.put("publication", null); //TODO pub_id goes here;
		return Arrays.asList(new String[]{query, searchTerm});
	}
	
	/**
	 * PURPOSE This method uses an input collection of taxon to 
	 * phenotype assertions to create a data structure, which is 
	 * passed to the calling {@link #getAnnotations()} method
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
		taxonTree = taxonomyBuilder.getTree();
		NodeDTO mrca = taxonTree.getMrca();
		if(group.equals("root")){
			List<List<String>> listOfEQCRLists = 
				taxonTree.getNodeToListOfEQCRListsMap().get(mrca);
			taxonToAssertionsMap.put(mrca, listOfEQCRLists);
		}else{
			taxonToAssertionsMap = processChildrenOfGroupNodeFromInput(taxonToAssertionsMap);
		}
		return taxonToAssertionsMap;
	}
	
	/**
	 * This is a helper method that processes the annotations from the tree and returns them to 
	 * calling {@link #generateTreeBasedDataStructureFromAssertions(Map, Collection)} method
	 * @param taxonToAssertionsMap - the node to assertions map to be processed
	 * @return the processed node to assertions map
	 * @throws PhenoscapeTreeAssemblyException
	 */
	private Map<NodeDTO, List<List<String>>> processChildrenOfGroupNodeFromInput(
			Map<NodeDTO,List<List<String>>> taxonToAssertionsMap) throws PhenoscapeTreeAssemblyException{
		NodeDTO groupNodeFromInput = ttoTaxonomy.getIdToNodeMap().get(group);
		TaxonTree tree = taxonomyBuilder.getTree();
		Set<NodeDTO> children = tree.getNodeToChildrenMap().get(groupNodeFromInput);
		
		if(children != null){
			for(NodeDTO child : children){
				List<List<String>> listOfEQCRLists = 
					tree.getNodeToListOfEQCRListsMap().get(child);
				taxonToAssertionsMap.put(child, listOfEQCRLists);
			}
		}
		else if(tree.getLeaves().contains(groupNodeFromInput)){
			taxonToAssertionsMap = new HashMap<NodeDTO, List<List<String>>>();
		}
		else if(tree.getNodeToChildrenMap().get(tree.getMrca()) == null){ //mrca does not have children
			NodeDTO mrcaNode = tree.getMrca();
			taxonToAssertionsMap.put(mrcaNode, tree.getNodeToListOfEQCRListsMap().get(mrcaNode));
		}
		else if(tree.getMrca() != null){ //MRCA is lower than the search nnode
			NodeDTO mrcaNode = tree.getMrca();
			taxonToAssertionsMap.put(mrcaNode, tree.getNodeToListOfEQCRListsMap().get(mrcaNode));
		}
		else{
			throw new PhenoscapeTreeAssemblyException("");
		}
		return taxonToAssertionsMap;
	}
	
	/**
	 * This method generates a simple Taxon to List of Phenotypes
	 * map from the input taxon to phenotype assertions, which is 
	 * passed to the calling {@link #getAnnotations()} method
	 * @param taxonToAssertionsMap - the map from taxa to phenotype assertions
	 * @param phenotypeColl - the input collection of taxon to phenotype assertions
	 * @return a data structure containing all the assertions
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
					phenotypeDTO.getReifId(), 
					phenotypeDTO.getRelatedEntityId(), 
					phenotypeDTO.getRelatedEntity()
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
	
	private Logger log() {
		return Logger.getLogger(this.getClass());
	}
}
