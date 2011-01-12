package org.phenoscape.ws.resource;

import java.sql.SQLException;

import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.model.PublicationTerm;
import org.restlet.data.Reference;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public class PublicationTermResource extends AbstractPhenoscapeResource {

    private String termID = null;

    @Override
    protected void doInit() throws ResourceException {
        super.doInit();
        this.termID = Reference.decode((String) (this.getRequestAttributes().get("publicationID")));
    }

    @Get("json")
    public Representation getJSONRepresentation() {
        try {
            final PublicationTerm publication = this.getDataStore().getPublicationTerm(this.termID);
            if (publication == null) {
                this.setStatus(Status.CLIENT_ERROR_NOT_FOUND);
                return null;
            }
            return new JsonRepresentation(this.translate(publication));
        } catch (JSONException e) {
            log().error("Failed to create JSON object for publication: " + this.termID, e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        } catch (SQLException e) {
            log().error("Database error querying for publication: " + this.termID, e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        }
    }

    private JSONObject translate(PublicationTerm publication) throws JSONException {
        final JSONObject json = TermResourceUtil.translateMinimal(publication);
        json.put("citation", publication.getCitation());
        json.put("abstract", publication.getAbstractText());
        json.put("doi", publication.getDoi());
        json.put("source", TermResourceUtil.translateMinimal(publication.getSource()));
        return json;
    }

}
