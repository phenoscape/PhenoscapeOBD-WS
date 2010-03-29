package org.obd.ws.resources;

import java.sql.SQLException;

import org.json.JSONException;
import org.json.JSONObject;
import org.obd.ws.application.OBDApplication;
import org.obd.ws.util.Queries;
import org.phenoscape.obd.OBDQuery;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public class KbRefreshTimestampResource extends AbstractOBDResource{

    private JSONObject jObjs;
    private OBDQuery obdq;
    private Queries queries;

    @Override
    protected void doInit() throws ResourceException {
        super.doInit();
        this.queries = (Queries)this.getContext().getAttributes().get(OBDApplication.QUERIES_STRING);
        this.jObjs = new JSONObject();
    }

    @Get("json")
    public Representation getJSONRepresentation() throws ResourceException {
        String timestamp;
        try{
            connectShardToDatabase();
            obdq = new OBDQuery(this.getShard(), queries);
            timestamp = getTimeStamp();
            assembleJSONObjectFromTimestamp(timestamp);
        } catch (SQLException sqle) {
            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, 
            "[SQL EXCEPTION] Something broke server side. Consult server logs");
            return null;
        } catch (ClassNotFoundException e) {
            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, 
            "[CLASS NOT FOUND EXCEPTION] Something broke server side. Consult server logs");
            return null;
        } catch (JSONException jsone) {
            getResponse().setStatus(Status.CLIENT_ERROR_UNPROCESSABLE_ENTITY,  
            "[JSON EXCEPTION] JSON object error.");
            log().error(jsone);
            return null;
        } finally {
            disconnectShardFromDatabase();
        }
        return new JsonRepresentation(this.jObjs);
    }

    private String getTimeStamp() throws SQLException {
        return obdq.executeTimestampQuery();
    }

    private void assembleJSONObjectFromTimestamp(String timestamp) throws JSONException {
        jObjs.put("timestamp", timestamp);
    }

}
