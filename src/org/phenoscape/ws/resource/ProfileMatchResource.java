package org.phenoscape.ws.resource;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.model.Vocab.TTO;
import org.phenoscape.obd.query.AnnotationsQueryConfig;
import org.phenoscape.obd.query.QueryException;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public class ProfileMatchResource extends AbstractPhenoscapeResource {
    
    private String taxonID;
    private AnnotationsQueryConfig phenotypes;
    
    @Override
    protected void doInit() throws ResourceException {
        super.doInit();
        this.taxonID = this.getFirstQueryValue("taxon");
        try {
            this.phenotypes = this.initializeQueryConfig(this.getJSONQueryValue("query", new JSONObject()));
        } catch (QueryException e) {
            log().error("Incorrectly formatted phenotype query", e);
            throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, e);
        } catch (JSONException e) {
            log().error("Incorrectly formatted phenotype query", e);
            throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, e);
        }
    }
    
    @Get("json")
    public Representation getJSONRepresentation() {
        try {
            final Map<String, Integer> matches;
            if (this.taxonID == null) {
                matches = this.getDataStore().getGreatestProfileMatchesForChildren(TTO.ROOT, this.phenotypes.getPhenotypes(), true);
            } else {
                matches = this.getDataStore().getGreatestProfileMatchesForChildren(taxonID, this.phenotypes.getPhenotypes(), false);
            }
            final JSONObject json = new JSONObject();
            final List<JSONObject> jsonMatches = new ArrayList<JSONObject>();
            for (Entry<String, Integer> match : matches.entrySet()) {
                final JSONObject taxon = new JSONObject();
                taxon.put("taxon_id", match.getKey());
                taxon.put("greatest_profile_match", match.getValue());
                jsonMatches.add(taxon);
            }
            json.put("matches", jsonMatches);
            return new JsonRepresentation(json);
        } catch (SQLException e) {
            log().error("Database error querying for profile matches", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        } catch (JSONException e) {
            log().error("Failed to create JSON object for profile matches", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        }
    }

}
