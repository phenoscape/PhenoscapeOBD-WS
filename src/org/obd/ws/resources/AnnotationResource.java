package org.obd.ws.resources;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.obd.query.impl.OBDSQLShard;
import org.obd.ws.application.OBDApplication;
import org.obd.ws.util.Queries;
import org.obd.ws.util.TTOTaxonomy;
import org.obd.ws.util.dto.AnnotationDTO;
import org.obd.ws.util.dto.NodeDTO;
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

/**
 * The purpose of this resource is to invoke a query
 * for a given row, which retrieves all the metadata
 * such as publications, curators, and notes in free text
 * about the row. These retrieved results are packaged
 * into a JSON Object, which is returned to the REST
 * endpoint
 */

public class AnnotationResource extends Resource {

	private final String driverName = "jdbc:postgresql://"; 
	
	private JSONObject jObjs;
	
    private OBDSQLShard obdsqlShard;
    private final String annotationId;
    
    private OBDQuery obdq;
    private Queries queries;
    
    private TTOTaxonomy ttoTaxonomy;
    
    /**
     * Constructor extends the default constructor
     * Initializes the Shard, Queries, OBDQuery and the JSON Object which
     * will be returned
     * @param context
     * @param request
     * @param response
     * @throws ClassNotFoundException 
     * @throws SQLException 
     */
    public AnnotationResource(Context context, Request request, Response response) throws SQLException, ClassNotFoundException {
        super(context, request, response);
        this.obdsqlShard = new OBDSQLShard();
        this.queries = (Queries)this.getContext().getAttributes().get(OBDApplication.QUERIES_STRING);
        this.ttoTaxonomy = (TTOTaxonomy)this.getContext().getAttributes().get(OBDApplication.TTO_TAXONOMY_STRING);
        this.annotationId = Reference.decode((String)(request.getAttributes().get("annotation_id")));
        this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
        this.jObjs = new JSONObject();
    }

    /**
     * The core method for this class. This method invokes
     * the {@link #getMetadata(String)} method to execute the query. The results are 
     * packaged into a JSON Object using the {@link #assembleJSONObjectFromAnnotations(List)}
     * method
     */
    public Representation represent(Variant variant) throws ResourceException {
    	
    	List<List<String[]>> annots;
    	
    	try{
    		connectShardToDatabase();
    		obdq = new OBDQuery(obdsqlShard, queries);
    		annots = getMetadata(annotationId);
    		assembleJSONObjectFromAnnotations(annots);
    	}catch(SQLException sqle){
    		getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, 
			"[SQL EXCEPTION] Something broke server side. Consult server logs");
    		return null;
    	} catch (ClassNotFoundException e) {
    		getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, 
			"[CLASS NOT FOUND EXCEPTION] Something broke server side. Consult server logs");
    		return null;
    	} catch(JSONException jsone){
    		getResponse().setStatus(Status.CLIENT_ERROR_UNPROCESSABLE_ENTITY,  
			"[JSON EXCEPTION] JSON object error.");
    		log().error(jsone);
            return null;
    	} finally{
    		disconnectShardFromDatabase();
    	}
    	return new JsonRepresentation(this.jObjs);
    }
    
    /**
     * This method reads in db connection parameters from app context before connecting
     * the Shard to the database
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
     * PURPOSE This method invokes the metadata query from
     * the Queries object and loads it for execution by invoking
     * the {@link OBDQuery#executeFreeTextQueryAndAssembleResults(Integer)} method. 
     * The results from this invocation are packaged into a data 
     * structure and returned to the calling {@link #represent(Variant)} method 
     * @param annotId - the id (INTEGER type) which connects the row
     * to all its metadata such as publications, curators names, 
     * character text, state text etc. This is cast into an Integer before the 
     * appropriate method from {@link OBDQuery} class is invoked
     * @return - a data structure which is input to the {@link #assembleJSONObjectFromAnnotations(List)} method
     * @throws SQLException
     */
    private List<List<String[]>> getMetadata(String annotId) throws SQLException{
    	
    	List<List<String[]>> results = new ArrayList<List<String[]>>();
    	List<String[]> annots;
    	
    	String taxonId, taxon, entityId, entity, qualityId, quality, 
    			publication, curators, charText, charComments, stateText, stateComments,
    			charNumber; 
    	
    	String[] annotIds = annotId.split(",");
    	
    	try{
    		for(String id : annotIds){
    			for(AnnotationDTO node : obdq.executeFreeTextQueryAndAssembleResults(Integer.parseInt(id))){
    				taxonId = node.getTaxonId();
    				taxon = node.getTaxon();
    				entityId = node.getEntityId();
    				entity = node.getEntity();
    				qualityId = node.getQualityId();
    				quality = node.getQuality();
			
    				publication = node.getPublication();
    				curators = node.getCurators();
    				charText = node.getCharText();
    				charComments = node.getCharComments();
    				stateText = node.getStateText();
    				stateComments = node.getStateComments();
    				charNumber = node.getCharNumber();
    				annots = new ArrayList<String[]>(); 
    				annots.add(new String[]{taxonId, taxon});
    				annots.add(new String[]{entityId, entity});
    				annots.add(new String[]{qualityId, quality});
    				annots.add(new String[]{publication});
    				annots.add(new String[]{curators});
    				annots.add(new String[]{charText, charComments, charNumber});
    				annots.add(new String[]{stateText, stateComments});
    			
    				results.add(annots);
    			}
    		}
    	}
    	catch (SQLException sqle){
    		sqle.printStackTrace();
    		log().error(sqle);
    		throw sqle;
    	}
    	return results;
    }
    
    /**
     * This method takes in a data structure of the annotations 
     * obtained from the {@link #getMetadata(String)} method and 
     * converts it into a formatted JSON object
     * @param annots
     * @throws JSONException
     */
    private void assembleJSONObjectFromAnnotations(List<List<String[]>> annots) throws JSONException{
    	List<JSONObject> sourceObjs = new ArrayList<JSONObject>();
    	JSONObject phenotypeObj, sourceObj;
    	JSONObject entityObj, taxonObj, qualityObj, rankObj;
    	
    	if(annots != null && annots.size() > 0){
			phenotypeObj = new JSONObject();
			for(List<String[]> annot : annots){
				entityObj = new JSONObject();
				taxonObj = new JSONObject();
				qualityObj = new JSONObject();
			
				sourceObj = new JSONObject();
			
				taxonObj.put("id", annot.get(0)[0]);
				taxonObj.put("name", annot.get(0)[1]);
				
				if(annot.get(0)[0].startsWith("TTO")){
					NodeDTO taxonDTO = new NodeDTO(annot.get(0)[0]);
					taxonDTO.setName(annot.get(0)[1]);
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
					taxonObj.put("rank", rankObj);
				}
				
				entityObj.put("id", annot.get(1)[0]);
				entityObj.put("name", annot.get(1)[1]);
				qualityObj.put("id", annot.get(2)[0]);
				qualityObj.put("name", annot.get(2)[1]);
			
				phenotypeObj.put("subject", taxonObj);
				phenotypeObj.put("entity", entityObj);
				phenotypeObj.put("quality", qualityObj);
			
				sourceObj.put("publication", annot.get(3)[0]);
				sourceObj.put("curated_by", annot.get(4)[0]);
				sourceObj.put("character_text", annot.get(5)[0]);
				sourceObj.put("character_comment", annot.get(5)[1]);
				sourceObj.put("character_number", annot.get(5)[2]);
				sourceObj.put("state_text", annot.get(6)[0]);
				sourceObj.put("state_comment", annot.get(6)[1]);
			
				sourceObjs.add(sourceObj);
			}
			jObjs.put("phenotype", phenotypeObj);
			jObjs.put("sources", sourceObjs);
    	}
    }
    
    /**
     * This method returns a logger (log4j)
     */
    private Logger log() {
        return Logger.getLogger(this.getClass());
    }
}
