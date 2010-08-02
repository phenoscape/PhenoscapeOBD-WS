package org.phenoscape.ws.resource;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.model.TaxonTerm;
import org.phenoscape.obd.model.Term;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Post;

public class BulkTermNameResource extends AbstractPhenoscapeResource {

    @Post("json")
    public Representation acceptJSON(Representation json) {
        log().debug(json.getClass());
        log().debug(json);
        try {
            final JSONObject input = (new JsonRepresentation(json)).getJsonObject();
            if (input.has("ids")) {
                final List<String> ids = this.extractIDs(input.getJSONArray("ids"));
                final JSONObject output = this.translate(this.getDataStore().getNamesForIDs(ids));
                return new JsonRepresentation(output);
            } else {
                throw new JSONException("Invalid JSON contents");
            }
        } catch (JSONException e) {
            log().error("Invalid JSON format", e);
            this.setStatus(Status.CLIENT_ERROR_BAD_REQUEST, e);
            return null;
        } catch (IOException e) {
            log().error("Error reading post content", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        } catch (SQLException e) {
            log().error("Error querying database", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        }
    }

    private List<String> extractIDs(JSONArray jsonIDs) throws JSONException {
        final List<String> ids = new ArrayList<String>();
        for (int i = 0; i < jsonIDs.length(); i++) {
            ids.add(jsonIDs.getString(i));
        }
        return ids;
    }

    private JSONObject translate(List<Term> terms) throws JSONException {
        final JSONObject json = new JSONObject();
        final JSONArray jsonTerms = new JSONArray();
        for (Term term : terms) {
            final JSONObject jsonTerm = new JSONObject();
            jsonTerm.put("id", term.getUID());
            jsonTerm.put("name", term.getLabel());
            if (term instanceof TaxonTerm) {
                final TaxonTerm taxon = (TaxonTerm)term;
                jsonTerm.put("extinct", taxon.isExtinct());
                if (taxon.getRank() != null) {
                    final JSONObject rank = new JSONObject();
                    rank.put("id", taxon.getRank().getUID());
                    rank.put("name", taxon.getRank().getLabel());
                    jsonTerm.put("rank", rank);
                }
            }
            jsonTerms.put(jsonTerm);
        }
        json.put("terms", jsonTerms);
        return json;
    }

}
