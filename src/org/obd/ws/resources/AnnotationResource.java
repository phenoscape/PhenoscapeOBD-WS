package org.obd.ws.resources;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.obd.query.Shard;
import org.obd.ws.util.dto.AnnotationDTO;
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

public class AnnotationResource extends Resource {

	/**
	 * The purpose of this resource is to invoke a query
	 * for a given row, which retrieves all the metadata
	 * such as publications, curators, and notes in free text
	 * about the row. These retrieved results are packaged
	 * into a JSON Object, which is returned to the REST
	 * endpoint
	 */
	private JSONObject jObjs;
	
    private final Shard shard;
    private final String annotationId;
    
    private OBDQuery obdq;
    
    /**
     * Constructor extends the default constructor
     * Initializes the Shard, Queries, OBDQuery and the JSON Object which
     * will be returned
     * @param context
     * @param request
     * @param response
     */
    public AnnotationResource(Context context, Request request, Response response) {
        super(context, request, response);
        this.shard = (Shard)this.getContext().getAttributes().get("shard");
        obdq = new OBDQuery(shard);
        this.annotationId = Reference.decode((String)(request.getAttributes().get("annotation_id")));
        this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
        this.jObjs = new JSONObject();
    }

    /**
     * This method returns a logger (log4j)
     * @return
     */
    private Logger log() {
        return Logger.getLogger(this.getClass());
    }
    
    /**
     * The core method for this class. This method invokes
     * the 'getMetadata' method, which indirectly sets up
     * the metadata query to be executed against the database. 
     * The results from the 'getmetadata' method are 
     * packaged into a JSON Object in this method
     */
    public Representation represent(Variant variant) throws ResourceException {
    	
    	List<List<String[]>> annots;
    	
    	List<JSONObject> sourceObjs = new ArrayList<JSONObject>();
    	JSONObject phenotypeObj, sourceObj;
    	JSONObject entityObj, taxonObj, qualityObj;
    	
    	try{
    		annots = getMetadata(Integer.parseInt(annotationId.trim()));
    	}
    	catch(SQLException sqle){
    		getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, 
			"[SQL EXCEPTION] Something broke server side. Consult server logs");
    		return null;
    	}
    	
    	try{
    		if(annots != null && annots.size() > 0){
    			List<String[]> annot = annots.get(0);
    			entityObj = new JSONObject();
    			taxonObj = new JSONObject();
    			qualityObj = new JSONObject();
    			
    			phenotypeObj = new JSONObject();
    			sourceObj = new JSONObject();
    			
    			taxonObj.put("id", annot.get(0)[0]);
    			taxonObj.put("name", annot.get(0)[1]);
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
    			sourceObj.put("state_text", annot.get(6)[0]);
    			sourceObj.put("state_comment", annot.get(6)[1]);
    			
    			sourceObjs.add(sourceObj);
    			
    			jObjs.put("phenotype", phenotypeObj);
    			jObjs.put("sources", sourceObjs);
    		}
    	}
    	catch(JSONException jsone){
    		getResponse().setStatus(Status.CLIENT_ERROR_UNPROCESSABLE_ENTITY,  
			"[JSON EXCEPTION] JSON object error.");
    		log().error(jsone);
            return null;
    	}
    	return new JsonRepresentation(this.jObjs);
    }
    
    /**
     * @PURPOSE This method invokes the metadata query from
     * the Queries object and loads it for execution by invoking
     * the 'executeFreeTextQueryAndAssembleResults' method from the
     * OBDQuery class. The results from this invocation are
     * packaged into a data structure and returned to the 
     * calling method
     * @param annotId - the id (INTEGER type) which connects the row
     * to all its metadata such as publications, curators names, 
     * character text, state text etc. This is cast into an Integer before the 
     * appropriate method from {@link OBDQuery} class is invoked
     * @return
     * @throws SQLException
     */
    private List<List<String[]>> getMetadata(Integer annotId) throws SQLException{
    	
    	List<List<String[]>> results = new ArrayList<List<String[]>>();
    	List<String[]> annots = new ArrayList<String[]>();;
    	
    	String taxonId, taxon, entityId, entity, qualityId, quality, 
    			publication, curators, charText, charComments, stateText, stateComments; 
    	
    	try{
    		for(AnnotationDTO node : obdq.executeFreeTextQueryAndAssembleResults(annotId)){
   			 	//Node properties stores every attribute of the given node with its value
				
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
			
    			annots.add(new String[]{taxonId, taxon});
    			annots.add(new String[]{entityId, entity});
    			annots.add(new String[]{qualityId, quality});
    			annots.add(new String[]{publication});
    			annots.add(new String[]{curators});
    			annots.add(new String[]{charText, charComments});
    			annots.add(new String[]{stateText, stateComments});
    			
    			results.add(annots);
    		}
    	}
    	catch (SQLException sqle){
    		sqle.printStackTrace();
    		log().error(sqle);
    		throw sqle;
    	}
    	return results;
    }
}
