package org.phenoscape.ws.resource;

import java.sql.SQLException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.model.LinkedTerm;
import org.phenoscape.obd.model.Synonym;
import org.restlet.data.Reference;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

/**
 * A resource proving term info limited to properties and relationships defined in source ontologies.
 */
public class TermInfoResource extends AbstractPhenoscapeResource {

    private String termID = null;

    @Override
    protected void doInit() throws ResourceException {
        super.doInit();
        this.termID = Reference.decode((String) (this.getRequestAttributes().get("termID")));
    }

    @Get("json")
    public Representation getJSONRepresentation() {
        try {
            final LinkedTerm term = this.getDataStore().getLinkedTerm(this.termID);
            if (term == null) {
                this.setStatus(Status.CLIENT_ERROR_NOT_FOUND);
                return null;
            }
            return new JsonRepresentation(this.translate(term));
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

    private JSONObject translate(LinkedTerm term) throws JSONException {
        final JSONObject json = this.translateMinimal(term);
        //        if (taxon.getParent() != null) {
        //            final JSONObject parent = this.translateMinimal(taxon.getParent());
        //            json.put("parent", parent);
        //        }
        //        final JSONArray children = new JSONArray();
        //        for (TaxonTerm childTaxon : taxon.getChildren()) {
        //            final JSONObject child = this.translateMinimal(childTaxon);
        //            children.put(child);
        //        }
        //        json.put("children", children);
        final JSONArray synonyms = new JSONArray();
        for (Synonym synonym : term.getSynonyms()) {
            final JSONObject synonymObj = new JSONObject();
            synonymObj.put("name", synonym.getLabel());
            synonymObj.put("lang", synonym.getLanguage());
            synonyms.put(synonymObj);
        }
        json.put("synonyms", synonyms);
        return json;
    }

    private JSONObject translateMinimal(LinkedTerm term) throws JSONException {
        final JSONObject json = new JSONObject();
        json.put("id", term.getUID());
        json.put("name", term.getLabel());
        return json;
    }

}
