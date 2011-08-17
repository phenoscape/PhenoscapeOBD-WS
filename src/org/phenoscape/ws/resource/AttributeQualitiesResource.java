package org.phenoscape.ws.resource;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.model.Term;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;

public class AttributeQualitiesResource extends AbstractPhenoscapeResource {

    @Get("json")
    public Representation getJSONRepresentation() {
        try {
            final List<Term> attributes = this.getDataStore().getQualityAttributes();
            final JSONObject json = new JSONObject();
            final List<JSONObject> jsonAttributes = new ArrayList<JSONObject>();
            for (Term attribute : attributes) {
                jsonAttributes.add(TermResourceUtil.translateMinimal(attribute));
            }
            json.put("attributes", jsonAttributes);
            return new JsonRepresentation(json);
        } catch (SQLException e) {
            log().error("Error querying database", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        } catch (JSONException e) {
            log().error("Error creating JSON", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        }
    }

}
