package org.phenoscape.ws.resource;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;

public class KBTimestampResource extends AbstractPhenoscapeResource {

    @Get("json")
    public Representation getJSONRepresentation() {
        try {
            return new JsonRepresentation(this.translate(this.getDataStore().getRefreshDate()));
        } catch (JSONException e) {
            log().error("Error creating JSON object for timestamp", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        } catch (ParseException e) {
            log().error("Unable to parse timestamp", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        } catch (SQLException e) {
            log().error("Database error querying for timestamp: ", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        }
    }
    
    private JSONObject translate(Date date) throws JSONException {
        final DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        final JSONObject json = new JSONObject();
        json.put("refresh_date", formatter.format(date));
        return json;
    }

}
