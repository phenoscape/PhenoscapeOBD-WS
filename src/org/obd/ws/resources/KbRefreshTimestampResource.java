package org.obd.ws.resources;

import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.obd.query.impl.OBDSQLShard;
import org.obd.ws.application.OBDApplication;
import org.obd.ws.util.Queries;
import org.phenoscape.obd.OBDQuery;
import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.ResourceException;
import org.restlet.resource.Variant;

public class KbRefreshTimestampResource extends Resource{

	private final String driverName = "jdbc:postgresql://"; 
	
	private JSONObject jObjs;
	
    private OBDSQLShard obdsqlShard;
    
    private OBDQuery obdq;
    private Queries queries;
    
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
    public KbRefreshTimestampResource(Context context, Request request, Response response) throws SQLException, ClassNotFoundException {
        super(context, request, response);
        this.obdsqlShard = new OBDSQLShard();
        this.queries = (Queries)this.getContext().getAttributes().get(OBDApplication.QUERIES_STRING);
        this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
        this.jObjs = new JSONObject();
    }
    
 public Representation represent(Variant variant) throws ResourceException {
    	
    	String timestamp;
    	
    	try{
    		connectShardToDatabase();
    		obdq = new OBDQuery(obdsqlShard, queries);
    		timestamp = getTimeStamp();
    		assembleJSONObjectFromTimestamp(timestamp);
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
    	disconnectShardFromDatabase();
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
    
    private String getTimeStamp() throws SQLException{
    	return obdq.executeTimestampQuery();
    }
    
    private void assembleJSONObjectFromTimestamp(String timestamp) throws JSONException{
    	JSONObject jObj = new JSONObject();
    	jObj.put("timestamp", timestamp);
    }
    /**
     * This method returns a logger (log4j)
     */
    private Logger log() {
        return Logger.getLogger(this.getClass());
    }
}
