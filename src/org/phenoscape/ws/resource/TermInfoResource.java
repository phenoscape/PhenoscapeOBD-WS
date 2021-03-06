package org.phenoscape.ws.resource;

import java.sql.SQLException;

import org.json.JSONException;
import org.phenoscape.obd.model.LinkedTerm;
import org.restlet.data.Reference;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

/**
 * A resource providing term info limited to the properties and relationships defined in source ontologies.
 */
public class TermInfoResource extends AbstractPhenoscapeResource {

    private String termID = null;

    @Override
    protected void doInit() throws ResourceException {
        super.doInit();
        this.termID = Reference.decode((String)(this.getRequestAttributes().get("termID")));
    }

    @Get("json")
    public Representation getJSONRepresentation() {
        try {
            final LinkedTerm term = this.getDataStore().getLinkedTerm(this.termID);
            if (term == null) {
                this.setStatus(Status.CLIENT_ERROR_NOT_FOUND);
                return null;
            }
            return new JsonRepresentation(TermResourceUtil.translate(term));
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

}
