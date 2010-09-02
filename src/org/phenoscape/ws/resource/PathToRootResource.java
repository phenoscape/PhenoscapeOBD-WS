package org.phenoscape.ws.resource;

import java.sql.SQLException;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.model.LinkedTerm;
import org.restlet.data.Reference;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public class PathToRootResource extends AbstractPhenoscapeResource {
    
    private String termID = null;

    @Override
    protected void doInit() throws ResourceException {
        super.doInit();
        this.termID = Reference.decode((String)(this.getRequestAttributes().get("termID")));
    }
    
    @Get("json")
    public Representation getJSONRepresentation() {
        try {
            final List<LinkedTerm> terms = this.getDataStore().getPathForTerm(this.termID);
            if (terms.isEmpty()) {
                this.setStatus(Status.CLIENT_ERROR_NOT_FOUND);
                return null;
            }
            return new JsonRepresentation(this.translate(terms));
        } catch (JSONException e) {
            log().error("Failed to create JSON object for term: " + this.termID, e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        } catch (SQLException e) {
            log().error("Database error querying for term: " + this.termID, e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        }
    }
    
    private JSONObject translate(List<LinkedTerm> terms) throws JSONException {
        final JSONObject json = new JSONObject();
        final JSONArray path = new JSONArray();
        for (LinkedTerm term : terms) {
            path.put(TermResourceUtil.translate(term));
        }
        json.put("path", path);
        return json;
    }

}
