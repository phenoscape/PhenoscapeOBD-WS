package org.phenoscape.ws.resource;

import java.sql.SQLException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.model.TaxonTerm;
import org.restlet.data.Reference;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

/**
 * A resource providing access to basic information about a taxon.
 * @author Jim Balhoff
 */
public class TaxonTermResource extends AbstractPhenoscapeResource {
    
    private String termID = null;
    
    @Override
    protected void doInit() throws ResourceException {
        super.doInit();
        this.termID = Reference.decode((String) (this.getRequestAttributes().get("termID")));
    }
    
    @Get("json")
    public Representation getJSONRepresentation() {
        try {
            final TaxonTerm taxon = this.getDataStore().getTaxonTerm(this.termID);
            return new JsonRepresentation(this.translate(taxon));
        } catch (JSONException e) {
            log().error("Failed to create JSON object for taxon", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        } catch (SQLException e) {
            log().error("Database error querying for taxon", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        }
    }
    
    private JSONObject translate(TaxonTerm taxon) throws JSONException {
        final JSONObject json = this.translateMinimal(taxon);
        if (taxon.getParent() != null) {
            final JSONObject parent = this.translateMinimal(taxon.getParent());
            json.put("parent", parent);
        }
        final JSONArray children = new JSONArray();
        for (TaxonTerm childTaxon : taxon.getChildren()) {
            final JSONObject child = this.translateMinimal(childTaxon);
            children.put(child);
        }
        json.put("children", children);
        return json;
    }
    
    private JSONObject translateMinimal(TaxonTerm taxon) throws JSONException {
        final JSONObject json = new JSONObject();
        json.put("id", taxon.getUID());
        json.put("name", taxon.getLabel());
        json.put("extinct", taxon.isExtinct());
        if (taxon.getRank() != null) {
            final JSONObject rank = new JSONObject();
            rank.put("id", taxon.getRank().getUID());
            rank.put("name", taxon.getRank().getLabel());
            json.put("rank", rank);
        }
        return json;
    }

}
