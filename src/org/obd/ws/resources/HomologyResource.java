package org.obd.ws.resources;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.obd.query.Shard;
import org.obd.ws.util.Queries;
import org.obd.ws.util.dto.HomologDTO;
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

public class HomologyResource extends Resource {
	
	private JSONObject jObjs;
	
    private final Shard shard;
    private final String termID;
    
    private OBDQuery obdq;
    private Queries queries;

    public HomologyResource(Context context, Request request, Response response) {
        super(context, request, response);
        this.shard = (Shard)this.getContext().getAttributes().get("shard");
        obdq = new OBDQuery(shard);
        queries = new Queries(shard);
        this.termID = Reference.decode((String)(request.getAttributes().get("termID")));
        this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
        this.jObjs = new JSONObject();
    }

    public Representation represent(Variant variant) throws ResourceException {

    	List<List<String[]>> results;
    	
    	JSONObject lhEntityObj, lhTaxonObj, rhEntityObj, rhTaxonObj, sourceObj;
    	JSONObject homologyObj, subjectObj, targetObj, evidenceObj;
    	List<JSONObject> homologyObjs = new ArrayList<JSONObject>();
    	
    	if(termID != null && !termID.startsWith("TAO:") && !termID.startsWith("ZFA:")){
			this.jObjs = null;
			getResponse().setStatus(
					Status.CLIENT_ERROR_BAD_REQUEST,
					"ERROR: The input parameter for entity "
							+ "is not a recognized anatomical entity");
			return null;
		}
    	
    	try{
    		results = getHomologyData(termID);
    	}
    	catch(SQLException sqle){
    		getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, 
			"[SQL EXCEPTION] Something broke server side. Consult server logs");
    		return null;
    	}
    	
    	try{
        	for(List<String[]> row : results){
        		lhEntityObj = new JSONObject();
        		lhTaxonObj = new JSONObject();
        		rhEntityObj = new JSONObject();
        		rhTaxonObj = new JSONObject();
        		sourceObj = new JSONObject();
        		
        		homologyObj = new JSONObject();
        		subjectObj = new JSONObject();
        		targetObj = new JSONObject();  		
        		evidenceObj = new JSONObject();
        		
        		lhEntityObj.put("id", row.get(0)[0]);
        		lhEntityObj.put("name", row.get(0)[1]);
        		lhTaxonObj.put("id", row.get(1)[0]);
        		lhTaxonObj.put("name", row.get(1)[1]);
        		
        		subjectObj.put("entity", lhEntityObj);
        		subjectObj.put("taxon", lhTaxonObj);
        		
        		rhEntityObj.put("id", row.get(2)[0]);
        		rhEntityObj.put("name", row.get(2)[1]);
        		rhTaxonObj.put("id", row.get(3)[0]);
        		rhTaxonObj.put("name", row.get(3)[1]);
        		
        		targetObj.put("entity", rhEntityObj);
        		targetObj.put("taxon", rhTaxonObj);
        		        		
        		sourceObj.put("publication", row.get(4)[0]);
        		
        		evidenceObj.put("id", row.get(5)[0]);
    			evidenceObj.put("name", row.get(5)[1]);
           		sourceObj.put("evidence", evidenceObj);
        		
        		homologyObj.put("subject", subjectObj);
        		homologyObj.put("target", targetObj);
        		homologyObj.put("source", sourceObj);
        		
        		homologyObjs.add(homologyObj);
        	}
        	
        	this.jObjs.put("homologies", homologyObjs);
    	}
    	catch(JSONException jsone){
    		getResponse().setStatus(Status.CLIENT_ERROR_UNPROCESSABLE_ENTITY,  
			"[JSON EXCEPTION] JSON object error.");
    		log().error(jsone);
            return null;
    	}
    	
    	return new JsonRepresentation(this.jObjs);
    }
    
    private Logger log() {
        return Logger.getLogger(this.getClass());
    }
    
    /**
     * @PURPOSE This method returns homology statements associated with the input term Id as a data structure
     * @INFO Data is retrieved in the form 
     * <Left Hand Entity><Left Hand Taxon><Right Hand Entity><Right Hand Taxon><Publication><Evidence Code><Evidence>
     * @param termId
     * @throws SQLException
     */
    private List<List<String[]>> getHomologyData(String termId) throws SQLException{
    	    	
    	//This map maps the homolgy node to the left and right hand entities and taxa and publication
    	List<List<String[]>> results = new ArrayList<List<String[]>>();
    	
    	// This list stores annotations about the homology data
    	List<String[]> annots;
    	
    	String lhEntityId, lhEntity, lhTaxonId, lhTaxon, rhEntityId,
    		rhEntity, rhTaxonId, rhTaxon, publication, evidenceCode, evidence,
    		sourceEntityId, sourceEntity, targetEntityId, targetEntity, 
    		sourceTaxonId, sourceTaxon, targetTaxonId, targetTaxon;
    	
    	String sqlQuery = queries.getHomologyQuery();
    	
    	log().trace(sqlQuery + "\t" + termId);
    	
    	try{
    		for(HomologDTO node: obdq.executeHomologyQueryAndAssembleResults(termId)){
    			 //Node properties stores every attribute of the given node with its value
				//extract all the attributes of the given node
				lhEntityId = node.getLhEntityId();
				lhEntity = node.getLhEntity();
				lhTaxonId = node.getLhTaxonId();
				lhTaxon = node.getLhTaxon();
				
				rhEntityId = node.getRhEntityId();
				rhEntity = node.getRhEntity();
				rhTaxonId = node.getRhTaxonId();
				rhTaxon = node.getRhTaxon();

				if(lhEntityId.equals(termId)){
					sourceEntityId = lhEntityId;
					sourceEntity = lhEntity;
					sourceTaxonId = lhTaxonId;
					sourceTaxon = lhTaxon;
					
					targetEntityId = rhEntityId;
					targetEntity = rhEntity;
					targetTaxonId = rhTaxonId;
					targetTaxon = rhTaxon;
				}
				else{
					sourceEntityId = rhEntityId;
					sourceEntity = rhEntity;
					sourceTaxonId = rhTaxonId;
					sourceTaxon = rhTaxon;
					
					targetEntityId = lhEntityId;
					targetEntity = lhEntity;
					targetTaxonId = lhTaxonId;
					targetTaxon = lhTaxon;
				}
				
				publication = node.getPublication();
				evidenceCode = node.getEvidenceCode();
				evidence = node.getEvidence();
				
				log().trace("RH Entity: " + rhEntityId + " [" + rhEntity + "] RH Taxon: " + rhTaxonId + "[" + rhTaxon + "] LH Entity: " +
						lhEntityId + "[" + lhEntity + "] LH Taxon: " + lhTaxonId + "[" + lhTaxon + "] PublIcation: " + publication + 
						"Evidence Code: " + evidenceCode + "Evidence: " + evidence);
				
				//we store the left and right hand entities and taxa here
				annots = new ArrayList<String[]>();
				annots.add(new String[]{sourceEntityId, sourceEntity});
				annots.add(new String[]{sourceTaxonId, sourceTaxon});
				annots.add(new String[]{targetEntityId, targetEntity});
				annots.add(new String[]{targetTaxonId, targetTaxon});
				annots.add(new String[]{publication});
				annots.add(new String[]{evidenceCode, evidence});
				
				results.add(annots);
    		}
    	}
    	catch (SQLException sqle){
    		log().error(sqle);
    		throw sqle;
    	}
    	return results;
    }

}
