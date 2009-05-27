package org.obd.ws.resources;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.obd.model.Node;
import org.obd.model.Statement;
import org.obd.query.Shard;
import org.obd.ws.util.Queries;
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

    	List<Map<String, List<String[]>>> results;
    	
    	JSONObject lhEntityObj, lhTaxonObj, rhEntityObj, rhTaxonObj, sourceObj;
    	JSONObject homologyObj, subjectObj, targetObj;
    	List<JSONObject> evidenceObjs;
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
    	
    	List<String[]> homologyComps;
    	List<String[]> evidenceCodes;
    	
    	Map<String, List<String[]>> homologyToCompMap = results.get(0);
    	Map<String, List<String[]>> homologyToSourcesMap = results.get(1);
    	try{
        	for(String key : homologyToCompMap.keySet()){
        		homologyComps = homologyToCompMap.get(key);
        		evidenceCodes = homologyToSourcesMap.get(key);
        		lhEntityObj = new JSONObject();
        		lhTaxonObj = new JSONObject();
        		rhEntityObj = new JSONObject();
        		rhTaxonObj = new JSONObject();
        		sourceObj = new JSONObject();
        		
        		homologyObj = new JSONObject();
        		subjectObj = new JSONObject();
        		targetObj = new JSONObject();
        		evidenceObjs = new ArrayList<JSONObject>();
        		
        		lhEntityObj.put("id", homologyComps.get(0)[0]);
        		lhEntityObj.put("name", homologyComps.get(0)[1]);
        		lhTaxonObj.put("id", homologyComps.get(1)[0]);
        		lhTaxonObj.put("name", homologyComps.get(1)[1]);
        		
        		subjectObj.put("entity", lhEntityObj);
        		subjectObj.put("taxon", lhTaxonObj);
        		
        		rhEntityObj.put("id", homologyComps.get(2)[0]);
        		rhEntityObj.put("name", homologyComps.get(2)[1]);
        		rhTaxonObj.put("id", homologyComps.get(3)[0]);
        		rhTaxonObj.put("name", homologyComps.get(3)[1]);
        		
        		targetObj.put("entity", rhEntityObj);
        		targetObj.put("taxon", rhTaxonObj);
        		        		
        		sourceObj.put("publication", homologyComps.get(4)[0]);
        		
        		for(String[] evidence : evidenceCodes){
        			JSONObject evidenceObj = new JSONObject();
        			evidenceObj.put("id", evidence[0]);
        			evidenceObj.put("name", evidence[1]);
        			evidenceObjs.add(evidenceObj);
        		}
        		
        		sourceObj.put("evidence", evidenceObjs);
        		
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
    private List<Map<String, List<String[]>>> getHomologyData(String termId) throws SQLException{
    	
    	List<Map<String, List<String[]>>> results = new ArrayList<Map<String, List<String[]>>>();
    	
    	Map<String, String> nodeProps;
    	
    	//This map maps the homolgy node to all the evidence codes
    	Map<String, List<String[]>> nodeToSourcesMap = new HashMap<String, List<String[]>>();
    	
    	//This map maps the homolgy node to the left and right hand entities and taxa and publication
    	Map<String, List<String[]>> nodeToMetadataMap = new HashMap<String, List<String[]>>();
    	
    	// This list stores annotations about the homology data
    	List<String[]> annots;
    	
    	String relId, target;
    	String nodeId, lhEntityId, lhEntity, lhTaxonId, lhTaxon, rhEntityId, 
    		rhEntity, rhTaxonId, rhTaxon, publication, evidenceCode, evidence;
    	
    	String sqlQuery = queries.getHomologyQuery();
    	
    	log().trace(sqlQuery + "\t" + termId);
    	
    	try{
    		for(Node node: obdq.executeHomologyQueryAndAssembleResults(sqlQuery, termId)){
    			 //Node properties stores every attribute of the given node with its value
    			nodeProps = new HashMap<String, String>();
				for(Statement stmt : node.getStatements()){
					relId = stmt.getRelationId();
					target = stmt.getTargetId();
					nodeProps.put(relId, target);
				} 
				//extract all the attributes of the given node
				nodeId = node.getId();
				lhEntityId = nodeProps.get("lhEntityId");
				lhEntity = nodeProps.get("lhEntity");
				lhTaxonId = nodeProps.get("lhTaxonId");
				lhTaxon = nodeProps.get("lhTaxon");
				
				rhEntityId = nodeProps.get("rhEntityId");
				rhEntity = nodeProps.get("rhEntity");
				rhTaxonId = nodeProps.get("rhTaxonId");
				rhTaxon = nodeProps.get("rhTaxon");
				publication = nodeProps.get("hasPublication");
				evidenceCode = nodeProps.get("hasEvidenceCode");
				evidence = nodeProps.get("hasEvidence");
				
				log().trace("RH Entity: " + rhEntityId + " [" + rhEntity + "] RH Taxon: " + rhTaxonId + "[" + rhTaxon + "] LH Entity: " +
						lhEntityId + "[" + lhEntity + "] LH Taxon: " + lhTaxonId + "[" + lhTaxon + "] PublIcation: " + publication + 
						"Evidence Code: " + evidenceCode + "Evidence: " + evidence);
				
				
				//kep track of evidence codes associated with the homology statement
				List<String[]> storedEvidenceCodes;
				
				if(nodeToSourcesMap.containsKey(nodeId))
					storedEvidenceCodes = nodeToSourcesMap.get(nodeId);
				else
					storedEvidenceCodes = new ArrayList<String[]>();

				storedEvidenceCodes.add(new String[]{evidenceCode, evidence});
				nodeToSourcesMap.put(nodeId, storedEvidenceCodes);
				
				//we store the left and right hand entities and taxa here
				annots = new ArrayList<String[]>();
				annots.add(new String[]{lhEntityId, lhEntity});
				annots.add(new String[]{lhTaxonId, lhTaxon});
				annots.add(new String[]{rhEntityId, rhEntity});
				annots.add(new String[]{rhTaxonId, rhTaxon});
				annots.add(new String[]{publication});
				
				nodeToMetadataMap.put(nodeId, annots);
    		}
    		results.add(nodeToMetadataMap);
    		results.add(nodeToSourcesMap);
    	}
    	catch (SQLException sqle){
    		log().error(sqle);
    		throw sqle;
    	}
    	return results;
    }

}
