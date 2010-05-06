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

import org.bbop.dataadapter.DataAdapterException;
import org.json.JSONException;
import org.json.JSONObject;
import org.obd.ws.application.OBDApplication;
import org.obd.ws.exceptions.PhenoscapeTreeAssemblyException;
import org.obd.ws.util.Queries;
import org.obd.ws.util.TTOTaxonomy;
import org.obd.ws.util.TaxonTree;
import org.obd.ws.util.TaxonomyBuilder;
import org.obd.ws.util.dto.NodeDTO;
import org.obd.ws.util.dto.PhenotypeDTO;
import org.phenoscape.obd.OBDQuery;
import org.restlet.data.Reference;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.representation.Variant;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public class PhenotypeDetailsResource extends AbstractOBDResource {

    private String subject_id;
    private String entity_id;
    private String character_id; 
    private String publication_id;
    private String type;
    private String group;

    private Map<String, String> parameters;
    private JSONObject jObjs;
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

    @Override
    protected void doInit() throws ResourceException {
        super.doInit();
        this.ttoTaxonomy = (TTOTaxonomy)this.getContext().getAttributes().get(OBDApplication.TTO_TAXONOMY_STRING);
        this.queries = (Queries)this.getContext().getAttributes().get(OBDApplication.QUERIES_STRING);

        if(this.getQuery().getFirstValue(SUBJECT_STRING) != null){
            this.subject_id = Reference.decode((String) (this.getQuery().getFirstValue(SUBJECT_STRING)));
        }
        if(this.getQuery().getFirstValue(ENTITY_STRING) != null){
            this.entity_id = Reference.decode((String)(this.getQuery().getFirstValue(ENTITY_STRING)));
        }
        if(this.getQuery().getFirstValue(QUALITY_STRING) != null){
            this.character_id = Reference.decode((String)(this.getQuery().getFirstValue(QUALITY_STRING)));
        }
        if(this.getQuery().getFirstValue(PUBLICATION_STRING) != null){
            this.publication_id = Reference.decode((String)(this.getQuery().getFirstValue(PUBLICATION_STRING)));
        }
        if(this.getQuery().getFirstValue(TYPE_STRING) != null){
            this.type = Reference.decode((String)(this.getQuery().getFirstValue(TYPE_STRING)));
        }
        if(this.getQuery().getFirstValue(GROUP_STRING) != null){
            this.group = Reference.decode((String)(this.getQuery().getFirstValue(GROUP_STRING)));
        }

        jObjs = new JSONObject();
        parameters = new HashMap<String, String>();
    }



    /**
     * This method is responsible for generating the object
     * which is output at the endpoint interface. It calls methods
     * which check the input form data, execute the query and 
     * process the results into a JSON object for delivery to
     * the endpoint interface 
     * 
     */
    @Get("json")
    public Representation getJSONRepresentation() throws ResourceException {

        Representation rep;
        Map<NodeDTO, List<PhenotypeDTO>> annots;

        try {
            this.connectShardToDatabase();
            if(!inputFormParametersAreValid()){
                this.jObjs = null;
                return null;
            }
            obdq = new OBDQuery(this.getShard(), queries);
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
     * PURPOSE: This method takes the nodes returned by OBDQuery class and packages them into a 
     * data structure, which will be processed by the caller {@link #represent()} method
     * @return a data structure containing the results of the query execution
     * @throws SQLException 
     * @throws IOException 
     * @throws DataAdapterException 
     * @throws PhenoscapeTreeAssemblyException 
     */
    private Map<NodeDTO, List<PhenotypeDTO>> 
    getAnnotations() 
    throws SQLException, IOException, DataAdapterException, PhenoscapeTreeAssemblyException{

        Map<NodeDTO, List<PhenotypeDTO>> taxonToAssertionsMap = new HashMap<NodeDTO, List<PhenotypeDTO>>();

        List<String> queryAndSearchTerm = assembleQueryAndSearchTerm();
        String query = queryAndSearchTerm.get(0);
        String searchTerm = queryAndSearchTerm.get(1);

        log().trace("Search Term: " + searchTerm + " Query: " + query);
        try{
            Collection<PhenotypeDTO> phenotypeColl = 
                obdq.executeQueryAndAssembleResults(query, searchTerm);
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
                if(this.getShard().getNode(parameters.get(key)) == null){
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
    (Map<NodeDTO, List<PhenotypeDTO>> annots) 
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
                                if(annots.get(taxonDTO).get(0).getReifIds().size() > 0) {
                                    subjectObj.put("leaf", true);
                                    subjectObj.put("annotated_taxa_count", subsumedTaxaCount + 1);
                                }
                                else{
                                    subjectObj.put("leaf", false);
                                    subjectObj.put("annotated_taxa_count", subsumedTaxaCount);
                                }
                }
                phenotypeObjs = new ArrayList<JSONObject>();
                for(PhenotypeDTO phenotype : annots.get(taxonDTO)){
                    String count = phenotype.getNumericalCount();
                    if(count == null)
                        count = "";
                    entityObj = new JSONObject();
                    entityObj.put("id", phenotype.getEntityId());
                    entityObj.put("name", phenotype.getEntity());
                    qualityObj = new JSONObject();
                    String qualityId = phenotype.getQualityId();
                    String quality = phenotype.getQuality();
                    String relatedEntityId = phenotype.getRelatedEntityId();
                    String relatedEntity = phenotype.getRelatedEntity();
                    if(relatedEntityId != null && relatedEntity != null){
                        qualityObj.put("id", qualityId + "^OBO_REL:towards(" + relatedEntityId + ")");
                        if(quality.trim().endsWith("from") || quality.trim().endsWith("to") || quality.trim().endsWith("with"))
                            qualityObj.put("name", quality + " " + relatedEntity);
                        else
                            qualityObj.put("name", quality + " towards " + relatedEntity);
                    }
                    else{
                        qualityObj.put("id", qualityId);
                        qualityObj.put("name", quality);
                    }
                    phenotypeObj = new JSONObject();
                    phenotypeObj.put("entity", entityObj);
                    phenotypeObj.put("quality", qualityObj);
                    phenotypeObj.put("count", count);
                    reifIdSet = phenotype.getReifIds();
                    if(reifIdSet.size() > 0)
                        phenotypeObj.put("id", reifIdSet);
                    else
                        phenotypeObj.put("id", "");
                    relatedEntityObj = new JSONObject();
                    relatedEntityObj.put("id", relatedEntityId);
                    relatedEntityObj.put("name", relatedEntity);
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
        if(subject_id != null){
            if(subject_id.contains("GENE"))
                query = queries.getGeneQuery(); 
            else 
                query = queries.getTaxonQuery();
            searchTerm = subject_id;
            if(entity_id != null)
                query += " AND dw.superentity_nid = (SELECT node_id FROM node WHERE uid = '" + entity_id + "')";
        }
        else{
            query = queries.getAnatomyDetailsQuery();
            /*	neither subject or entity are provided. so we use the root TAO term
             * which returns every phenotype in the database
             */
            searchTerm = (entity_id != null ? entity_id : "TAO:0100000");
        }
        if(character_id != null){
            query += " AND p1.character_uid = '" + character_id + "'";
        }
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
    private Map<NodeDTO, List<PhenotypeDTO>> generateTreeBasedDataStructureFromAssertions (
            Map<NodeDTO, List<PhenotypeDTO>> taxonToAssertionsMap, 
            Collection<PhenotypeDTO> phenotypeColl) 
            throws IOException, DataAdapterException, PhenoscapeTreeAssemblyException{
        taxonomyBuilder = new TaxonomyBuilder(ttoTaxonomy, phenotypeColl);
        taxonTree = taxonomyBuilder.getTree();
        NodeDTO mrca = taxonTree.getMrca();
        if(group.equals("root")){
            List<PhenotypeDTO> listOfEQCRLists = 
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
    private Map<NodeDTO, List<PhenotypeDTO>> processChildrenOfGroupNodeFromInput(
            Map<NodeDTO,List<PhenotypeDTO>> taxonToAssertionsMap) throws PhenoscapeTreeAssemblyException{
        NodeDTO groupNodeFromInput = ttoTaxonomy.getIdToNodeMap().get(group);
        TaxonTree tree = taxonomyBuilder.getTree();
        Set<NodeDTO> children = tree.getNodeToChildrenMap().get(groupNodeFromInput);

        if(children != null){
            for(NodeDTO child : children){
                List<PhenotypeDTO> listOfEQCRLists = 
                    tree.getNodeToListOfEQCRListsMap().get(child);
                taxonToAssertionsMap.put(child, listOfEQCRLists);
            }
        }
        else if(tree.getLeaves().contains(groupNodeFromInput)){
            taxonToAssertionsMap = new HashMap<NodeDTO, List<PhenotypeDTO>>();
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
    private Map<NodeDTO, List<PhenotypeDTO>> generateSimpleDataStructureFromAssertions(Map<NodeDTO, List<PhenotypeDTO>> taxonToAssertionsMap, Collection<PhenotypeDTO> phenotypeColl){
        for(PhenotypeDTO phenotypeDTO : phenotypeColl){
            NodeDTO taxonDTO = new NodeDTO(phenotypeDTO.getTaxonId());
            taxonDTO.setName(phenotypeDTO.getTaxon());

            List<PhenotypeDTO> annotations = taxonToAssertionsMap.get(taxonDTO);
            if(annotations == null)
                annotations = new ArrayList<PhenotypeDTO>();
            annotations.add(phenotypeDTO);
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
