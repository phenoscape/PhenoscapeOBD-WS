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
import org.obd.ws.util.dto.HomologDTO;
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

public class HomologyResource extends Resource {
	private final String driverName = "jdbc:postgresql://"; 
	private JSONObject jObjs;
	
    private OBDSQLShard obdsqlShard;
    private final String termID;
    
    private OBDQuery obdq;
    private Queries queries;
    private TTOTaxonomy ttoTaxonomy;
    
    /**
     * Constructor method
     * Instantiates all instance variables
     * @param context
     * @param request
     * @param response
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public HomologyResource(Context context, Request request, Response response) throws SQLException, ClassNotFoundException {
        super(context, request, response);
        this.obdsqlShard = new OBDSQLShard();
        this.queries = (Queries)this.getContext().getAttributes().get(OBDApplication.QUERIES_STRING);
        this.ttoTaxonomy = (TTOTaxonomy)this.getContext().getAttributes().get(OBDApplication.TTO_TAXONOMY_STRING);
        this.termID = Reference.decode((String)(request.getAttributes().get("termID")));
        this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
        this.jObjs = new JSONObject();
    }

    /**
     * The most important method in this class. This method 
     * - Checks for validity of input term
     * - connects Shard to the database 
     * - calls {@link #getHomologyData(String)} to execute query for homology data
     * - calls {@link #assembleJSONObjectFromResults(List)} method to assemble
     *  results of query execution into a JSON object
     */
    public Representation represent(Variant variant) throws ResourceException {

    	List<List<String[]>> results;
    	
    	if(termID != null && !termID.startsWith("TAO:") && !termID.startsWith("ZFA:")){
			this.jObjs = null;
			getResponse().setStatus(
					Status.CLIENT_ERROR_BAD_REQUEST,
					"ERROR: The input parameter for entity "
							+ "is not a recognized anatomical entity");
			return null;
		}
    	
    	try{
    		connectShardToDatabase();
    		obdq = new OBDQuery(obdsqlShard, queries);
    		results = getHomologyData(termID);
    		this.assembleJSONObjectFromResults(results);
    	} catch(SQLException sqle){
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
    	
    	disconnectShardFromDatabase();
    	return new JsonRepresentation(this.jObjs);
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
     * PURPOSE This method returns homology statements associated with the input term Id as a data structure
     * Data is retrieved in the form\n 
     * <Left Hand Entity><Left Hand Taxon><Right Hand Entity><Right Hand Taxon><Publication><Evidence Code><Evidence>\n
     * @param termId
     * @return a data structure containing the results of the homology query execution
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

    /**
     * This method assembles a JSON object from the input data structure
     * @param results - the data structure containing the results of the query execution. This
     * data structure is obtained from the {@link #getHomologyData(String)} method
     * @throws JSONException
     */
    private void assembleJSONObjectFromResults(List<List<String[]>> results) throws JSONException{
    	JSONObject lhEntityObj, lhTaxonObj, rhEntityObj, rhTaxonObj, sourceObj, lhRankObj, rhRankObj;
    	JSONObject homologyObj, subjectObj, targetObj, evidenceObj;
    	List<JSONObject> homologyObjs = new ArrayList<JSONObject>();
    	
    	for(List<String[]> row : results){
    		lhEntityObj = new JSONObject();
    		lhTaxonObj = new JSONObject();
    		lhRankObj = new JSONObject();
    		rhEntityObj = new JSONObject();
    		rhTaxonObj = new JSONObject();
    		rhRankObj = new JSONObject();
    		sourceObj = new JSONObject();
    		
    		homologyObj = new JSONObject();
    		subjectObj = new JSONObject();
    		targetObj = new JSONObject();  		
    		evidenceObj = new JSONObject();
    		
    		lhEntityObj.put("id", row.get(0)[0]);
    		lhEntityObj.put("name", row.get(0)[1]);
    		lhTaxonObj.put("id", row.get(1)[0]);
    		lhTaxonObj.put("name", row.get(1)[1]);
    		
    		NodeDTO lhTaxon = new NodeDTO(row.get(1)[0]);
    		lhTaxon.setName(row.get(1)[1]);
    		
    		NodeDTO lhRank = ttoTaxonomy.getTaxonToRankMap().get(lhTaxon);
    		if(lhRank != null){
    			lhRankObj.put("id", lhRank.getId());
    			lhRankObj.put("name", lhRank.getName());
    		}
    		else{
    			lhRankObj.put("id", "");
    			lhRankObj.put("name", "");
    		}
    		lhTaxonObj.put("rank", lhRankObj);
    		
    		subjectObj.put("entity", lhEntityObj);
    		subjectObj.put("taxon", lhTaxonObj);
    		
    		rhEntityObj.put("id", row.get(2)[0]);
    		rhEntityObj.put("name", row.get(2)[1]);
    		rhTaxonObj.put("id", row.get(3)[0]);
    		rhTaxonObj.put("name", row.get(3)[1]);
    		
    		NodeDTO rhTaxon = new NodeDTO(row.get(3)[0]);
    		rhTaxon.setName(row.get(3)[1]);
    		
    		NodeDTO rhRank = ttoTaxonomy.getTaxonToRankMap().get(rhTaxon);
    		if(rhRank != null){
    			rhRankObj.put("id", rhRank.getId());
    			rhRankObj.put("name", rhRank.getName());
    		}
    		else{
    			rhRankObj.put("id", "");
    			rhRankObj.put("name", "");
    		}
    		rhTaxonObj.put("rank", rhRankObj);
    		
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
    
    private Logger log() {
        return Logger.getLogger(this.getClass());
    }
}
