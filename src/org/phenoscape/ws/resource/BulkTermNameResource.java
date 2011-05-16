package org.phenoscape.ws.resource;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.model.LinkedTerm;
import org.phenoscape.obd.model.Relationship;
import org.phenoscape.obd.model.TaxonTerm;
import org.phenoscape.obd.model.Term;
import org.phenoscape.obd.query.PhenoscapeDataStore.POSTCOMP_OPTION;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Post;

//TODO should change output to a map instead of a list
public class BulkTermNameResource extends AbstractPhenoscapeResource {
    
    public static String RENDER_POSTCOMPOSITIONS = "render_postcompositions";
    POSTCOMP_OPTION postCompOption = POSTCOMP_OPTION.SIMPLE_LABEL;
    private static final Map<String,POSTCOMP_OPTION> POSTCOMP_OPTIONS = new HashMap<String,POSTCOMP_OPTION>();
    static {
        POSTCOMP_OPTIONS.put("structure", POSTCOMP_OPTION.STRUCTURE);
        POSTCOMP_OPTIONS.put("semantic_label", POSTCOMP_OPTION.SEMANTIC_LABEL);
        POSTCOMP_OPTIONS.put("simple_label", POSTCOMP_OPTION.SIMPLE_LABEL);
        POSTCOMP_OPTIONS.put("none", POSTCOMP_OPTION.NONE);
    }

    @Post("json")
    public Representation acceptJSON(Representation json) {
        try {
            final JSONObject input = (new JsonRepresentation(json)).getJsonObject();
            if (input.has(RENDER_POSTCOMPOSITIONS)) {
                final String option = input.getString(RENDER_POSTCOMPOSITIONS);
                if (!POSTCOMP_OPTIONS.containsKey(option)) {
                    throw new JSONException("Invalid JSON contents");
                }
                this.postCompOption = POSTCOMP_OPTIONS.get(option);
            }
            if (input.has("ids")) {
                final Set<String> ids = this.extractIDs(input.getJSONArray("ids"));
                final JSONObject output = this.translate(this.getDataStore().getNamesForIDs(ids, this.postCompOption));
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

    private Set<String> extractIDs(JSONArray jsonIDs) throws JSONException {
        final Set<String> ids = new HashSet<String>();
        for (int i = 0; i < jsonIDs.length(); i++) {
            ids.add(jsonIDs.getString(i));
        }
        return ids;
    }

    private JSONObject translate(List<Term> terms) throws JSONException {
        final JSONObject json = new JSONObject();
        final JSONArray jsonTerms = new JSONArray();
        for (Term term : terms) {
            jsonTerms.put(this.translate(term));
        }
        json.put("terms", jsonTerms);
        return json;
    }
    
    private JSONObject translate(Term term) throws JSONException {
        final JSONObject jsonTerm = new JSONObject();
        jsonTerm.put("id", term.getUID());
        if ((term.getLabel() == null) && (term instanceof LinkedTerm)) {
            jsonTerm.put("parents", this.translateRelationships(((LinkedTerm)term).getSubjectLinks()));
        } else {
            jsonTerm.put("name", term.getLabel());
            jsonTerm.put("source", this.createBasicJSONTerm(term.getSource()));
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
        }
        return jsonTerm;
    }

    private JSONArray translateRelationships(Set<Relationship> relationships) throws JSONException {
        final JSONArray links = new JSONArray();
        for (Relationship relationship : relationships) {
            final JSONObject link = new JSONObject();
            link.put("target", this.translate(relationship.getOther()));
            link.put("relation", this.createBasicJSONTerm(relationship.getPredicate()));
            links.put(link);
        }
        return links;
    }
}
