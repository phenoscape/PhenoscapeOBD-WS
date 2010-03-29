package org.obd.ws.resources;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;
import org.obd.ws.application.OBDApplication;
import org.obd.ws.util.Queries;
import org.obd.ws.util.dto.PhenotypeDTO;
import org.phenoscape.dw.queries.StoredProceduresForPhenotypeSummaries;
import org.phenoscape.obd.OBDQuery;
import org.restlet.data.Reference;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public class PhenotypeSummaryResource extends AbstractOBDResource {

    private String subject_id;
    private String entity_id;
    private String quality_id; 
    private String publication_id;
    private int examples_count = 0;

    private Map<String, String> parameters;

    private JSONObject jObjs;
    private OBDQuery obdq;
    private Queries queries;
    private StoredProceduresForPhenotypeSummaries spps; 

    private static final String SUBJECT_STRING = "subject";
    private static final String ENTITY_STRING = "entity";
    private static final String QUALITY_STRING = "quality";
    private static final String PUBLICATION_STRING = "publication";
    private static final String EXAMPLES_STRING = "examples";

    @Override
    protected void doInit() throws ResourceException {
        super.doInit();
        this.queries = (Queries)this.getContext().getAttributes().get(OBDApplication.QUERIES_STRING);
        if (this.getQuery().getFirstValue(SUBJECT_STRING) != null) {
            this.subject_id = Reference.decode((String) (this.getQuery().getFirstValue(SUBJECT_STRING)));
        }
        if (this.getQuery().getFirstValue(ENTITY_STRING) != null) {
            this.entity_id = Reference.decode((String)(this.getQuery().getFirstValue(ENTITY_STRING)));
        }
        if (this.getQuery().getFirstValue(QUALITY_STRING) != null) {
            this.quality_id = Reference.decode((String)(this.getQuery().getFirstValue(QUALITY_STRING)));
        }
        if (this.getQuery().getFirstValue(PUBLICATION_STRING) != null) {
            this.publication_id = Reference.decode((String)(this.getQuery().getFirstValue(PUBLICATION_STRING)));
        }
        if (this.getQuery().getFirstValue(EXAMPLES_STRING) != null) {
            this.examples_count = Integer.parseInt(Reference.decode((String)(this.getQuery().getFirstValue(EXAMPLES_STRING))));
        }
        jObjs = new JSONObject();
        parameters = new HashMap<String, String>();
    }


    /**
     * The control method for the REST resource. 
     * All the checks for correct parameter values
     * are done here before the data processing methods
     * are invoked. Then the {@link #getAnnotationSummary(String, String, String, String)} method
     * is invoked to execute the query. The {@link #assembleJsonObjectFromResults(Map)}
     * is invoked to assemble the results of the query execution
     */
    @Get("json")
    public Representation getJSONRepresentation() throws ResourceException {
        Representation rep;
        Map<String, Map<String, List<Set<String>>>> ecAnnots;
        try{
            this.connectShardToDatabase();
            if(!inputFormParametersAreValid()){
                getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND,
                "No annotations were found with the specified search parameters");
                this.disconnectShardFromDatabase();
                return null;
            }
            obdq = new OBDQuery(this.getShard(), queries);
            spps = new StoredProceduresForPhenotypeSummaries(this.getShard());
            ecAnnots = getAnnotationSummary(subject_id, entity_id, quality_id, publication_id);
            this.assembleJsonObjectFromResults(ecAnnots);
        } catch(SQLException sqle){
            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, 
            "[SQL EXCEPTION] Something broke server side. Consult server logs");
            return null;
        } catch (ClassNotFoundException e) {
            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, 
            "[CLASS NOT FOUND EXCEPTION] Something broke server side. Consult server logs");
            return null;
        } catch(JSONException jsone){
            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, 
            "[RESOURCE EXCEPTION] Something broke server side. Consult server logs");
            log().error(jsone);
            throw new ResourceException(jsone);
        } finally{
            this.disconnectShardFromDatabase();
        }
        rep = new JsonRepresentation(this.jObjs);
        return rep;
    }

    /**
     * This method checks if the input parametesrs from the form are valid
     * @return a boolean to indicate if input form parameters are valid
     */
    private boolean inputFormParametersAreValid(){

        if (subject_id != null && !subject_id.startsWith("TTO:") && !subject_id.contains("GENE")) {
            this.jObjs = null;
            getResponse().setStatus(
                    Status.CLIENT_ERROR_BAD_REQUEST,
                    "ERROR: The input parameter for subject "
                    + "is not a recognized taxon or gene");
            return false;
        }
        if(quality_id != null && !quality_id.startsWith("PATO:")){
            this.jObjs = null;
            getResponse().setStatus(
                    Status.CLIENT_ERROR_BAD_REQUEST,
                    "ERROR: The input parameter for quality "
                    + "is not a recognized PATO quality");
            return false;
        }

        //TODO Publication ID check

        parameters.put("entity_id", entity_id);
        parameters.put("quality_id", quality_id);
        parameters.put("subject_id", subject_id);
        parameters.put("publication_id", publication_id);

        for(String key : parameters.keySet()){
            if(parameters.get(key) != null){
                if(this.getShard().getNode(parameters.get(key)) == null){
                    return false;
                }
            }
        }
        return true;
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
     * @return a data structure containing the results of the query execution
     * @throws SQLException 
     */
    private Map<String, Map<String, List<Set<String>>>> getAnnotationSummary(String subject_id, String entity_id, String char_id, String pub_id) throws SQLException {
        /* This is a data structure to keep track of user specified filter options. */
        Collection<PhenotypeDTO> nodes; 
        String query, searchTerm;

        String geneSummaryStoredProc = StoredProceduresForPhenotypeSummaries.invokeStoredProcedureForGeneSummary;
        String taxonSummaryStoredProc = StoredProceduresForPhenotypeSummaries.invokeStoredProcedureForTaxonSummary;

        if(subject_id != null){
            searchTerm = subject_id;
            if(subject_id.contains("GENE"))
                nodes = spps.executeStoredProcedureAndAssembleResults(geneSummaryStoredProc, searchTerm);
            else
                nodes = spps.executeStoredProcedureAndAssembleResults(taxonSummaryStoredProc, searchTerm);
        }
        else{
            query = queries.getAnatomyQuery();
            searchTerm = (entity_id != null ? entity_id : "TAO:0100000"); //use root  of TAO if no params are specified
            nodes = obdq.executeQueryAndAssembleResults(query, searchTerm);
        }
        return this.summarizeResultsByEntityCharacter(nodes);
    }

    private Map<String, Map<String, List<Set<String>>>> summarizeResultsByEntityCharacter(Collection<PhenotypeDTO> nodes){
        Map<String, Map<String, List<Set<String>>>> entityCharAnnots = 
            new HashMap<String, Map<String, List<Set<String>>>>(); 
        Map<String, List<Set<String>>> charAnnots;
        List<Set<String>> annots;

        String characterId = null, entityId = null, character = null, entity = null;

        for(PhenotypeDTO node : nodes){

            characterId = node.getCharacterId();
            character = node.getCharacter();

            entityId = node.getEntityId();
            entity = node.getEntity();

            if(entityCharAnnots.keySet().contains(entityId + "\t" + entity)){
                charAnnots = entityCharAnnots.get(entityId + "\t" + entity);
                if(charAnnots.keySet().contains(characterId + "\t" + character)){
                    annots = charAnnots.get(characterId + "\t" + character);
                }
                else{
                    annots = this.createDataStructure();
                }

                annots = this.populateDataStructure(node, annots);
                charAnnots.put(characterId + "\t" + character, annots);
            }
            else{
                charAnnots = new HashMap<String, List<Set<String>>>();
                annots = this.createDataStructure();

                annots = this.populateDataStructure(node, annots);
                charAnnots.put(characterId + "\t" + character, annots);
            }
            entityCharAnnots.put(entityId + "\t" + entity, charAnnots);
        }
        return entityCharAnnots;
    }

    private List<Set<String>> createDataStructure(){
        List<Set<String>> annots = new ArrayList<Set<String>>();
        Set<String> gAnnots = new HashSet<String>();
        Set<String> tAnnots = new HashSet<String>();
        Set<String> qAnnots = new HashSet<String>();
        Set<String> pAnnots = new HashSet<String>();

        annots.add(0, gAnnots);
        annots.add(1, tAnnots);
        annots.add(2, qAnnots);
        annots.add(3, pAnnots);

        return annots;
    }

    private List<Set<String>> populateDataStructure(PhenotypeDTO node, List<Set<String>> annots){
        String quality = node.getQuality();
        String qualityId = node.getQualityId();

        String relatedEntity = node.getRelatedEntity();
        String relatedEntityId = node.getRelatedEntityId();

        String taxonId = node.getTaxonId();
        String taxon = node.getTaxon();

        String count = node.getNumericalCount();
        String publication = node.getPublication();

        Set<String> gAnnots = annots.get(0);
        Set<String> tAnnots = annots.get(1);
        Set<String> qAnnots = annots.get(2);
        Set<String> pAnnots = annots.get(3);

        if(relatedEntityId != null && relatedEntity != null){
            if(quality.trim().endsWith("from") || quality.trim().endsWith("to") || quality.trim().endsWith("with"))
                qAnnots.add(qualityId + "^OBO_REL:towards(" + relatedEntityId + ")\t" + quality + " " + relatedEntity);
            else
                qAnnots.add(qualityId + "^OBO_REL:towards(" + relatedEntityId + ")\t" + quality + " towards " + relatedEntity);
        }
        else if(count != null)
            qAnnots.add(qualityId + "^PHENOSCAPE:has_count(" + count + ")\t" + quality + " of " + count);
        else
            qAnnots.add(qualityId + "\t" + quality);
        if(taxonId.contains("GENE")){
            gAnnots.add(taxonId + "\t" + taxon);
        }
        else{
            tAnnots.add(taxonId + "\t" + taxon);
        }
        if(publication != null){
            pAnnots.add(publication);
        }
        annots.add(0, gAnnots);
        annots.add(1, tAnnots);
        annots.add(2, qAnnots);
        annots.add(3, pAnnots);

        return annots;
    }
    /**
     * This method assembles a JSON object from the input data structure
     * @param ecAnnots - this is the input data structure with the results of the phenotype summary query
     * @throws JSONException
     */
    private void assembleJsonObjectFromResults(Map<String, Map<String, List<Set<String>>>> ecAnnots) throws JSONException{
        JSONObject genesObj, taxaObj, qualitiesObj, exampleObj, publicationsObj; 
        JSONObject ecObject, eObject, cObject;
        List<JSONObject> gExampleObjs, tExampleObjs, qExampleObjs, pExampleObjs;
        List<JSONObject> ecObjects = new ArrayList<JSONObject>();

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
                publicationsObj = new JSONObject();
                Set<String> genesSet = cAnnots.get(charId).get(0);
                Set<String> taxaSet = cAnnots.get(charId).get(1);
                Set<String> qualitiesSet = cAnnots.get(charId).get(2);
                Set<String> publicationsSet = cAnnots.get(charId).get(3);
                gExampleObjs = new ArrayList<JSONObject>();
                tExampleObjs = new ArrayList<JSONObject>();
                qExampleObjs = new ArrayList<JSONObject>();
                pExampleObjs = new ArrayList<JSONObject>();
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
                publicationsObj.put("count", publicationsSet.size());
                Iterator<String> pit = publicationsSet.iterator();
                for(int i = 0; i < (Math.min(examples_count, publicationsSet.size())); i++){
                    String publication = pit.next();
                    exampleObj = new JSONObject();
                    exampleObj.put("title", publication);
                    pExampleObjs.add(exampleObj);
                }
                publicationsObj.put("examples", pExampleObjs);
                ecObject.put("qualities", qualitiesObj);
                ecObject.put("taxa", taxaObj);
                ecObject.put("genes", genesObj);
                ecObject.put("publications", publicationsObj);
                ecObjects.add(ecObject);
            }
        }
        this.jObjs.put("characters", ecObjects);
    }

}
