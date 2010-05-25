package org.phenoscape.ws.resource;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.model.MatchResult;
import org.phenoscape.obd.model.Vocab.GO;
import org.phenoscape.obd.model.Vocab.PATO;
import org.phenoscape.obd.model.Vocab.TAO;
import org.phenoscape.obd.model.Vocab.TTO;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public class AutocompleteResource extends AbstractPhenoscapeResource {
    
    private boolean matchName = true;
    private boolean matchSynonym = false;
    private boolean matchDefinition = false;
    private String searchText = null;
    private int limit = 0;
    
    private static final Map<String, String[]> prefixes = new HashMap<String, String[]>();
    static {
        prefixes.put("tao", new String[] {TAO.NAMESPACE});
        prefixes.put("go", new String[] {GO.NAMESPACE, GO.BP_NAMESPACE, GO.CC_NAMESPACE, GO.MF_NAMESPACE});
        prefixes.put("pato", new String[] {PATO.NAMESPACE});
        prefixes.put("tto", new String[] {TTO.NAMESPACE});
        //prefixes.put("zfin", ""); //TODO
    }
    
    @Override
    protected void doInit() throws ResourceException {
        super.doInit();
        this.searchText = this.getFirstQueryValue("text");
        this.matchName = this.getBooleanQueryValue("name", this.matchName);
        this.matchSynonym = this.getBooleanQueryValue("syn", this.matchSynonym);
        this.matchDefinition = this.getBooleanQueryValue("def", this.matchDefinition);
        this.limit = this.getIntegerQueryValue("limit", this.limit);
    }
    
    @Get("json")
    public Representation getJSONRepresentation() {
        try {
            if (this.searchText == null) {
                this.setStatus(Status.CLIENT_ERROR_BAD_REQUEST, "A search term must be provided.");
                return null;
            } else {
                return new JsonRepresentation(this.translate(this.queryForMatches()));   
            }
        } catch (JSONException e) {
            log().error("Failed to create JSON object", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        } catch (SQLException e) {
            log().error("Database error querying for autocomplete", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        }
    }

    private JSONObject translate(MatchResult result) throws JSONException, SQLException {
        final JSONObject json = new JSONObject();
        //TODO
        return json;
    }
    
    private MatchResult queryForMatches() {
        //TODO
        //return this.getDataStore().query...
        return null;
    }
    
}
