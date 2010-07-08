package org.phenoscape.ws.resource;

import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public class GeneAnnotationsResource extends AbstractPhenoscapeResource {

    private JSONObject query = null;

    @Override
    protected void doInit() throws ResourceException {
        super.doInit();
        final String jsonQuery = this.getFirstQueryValue("query");
        if (jsonQuery != null) {
            try {
                this.query = new JSONObject(jsonQuery);
            } catch (JSONException e) {
                throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, e);
            }
        }
    }

    @Get("json")
    public Representation getJSONRepresentation() {
        return new JsonRepresentation(this.getDummyData());
    }

    private JSONObject getDummyData() {
        try {
            return  new JSONObject("{\"total\":5478,\"annotations\":[{\"gene\":{\"id\":\"TAO0001514\",\"name\":\"brph1\"},\"entity\":{\"id\":\"TAO0001516\",\"name\":\"basihyalbone\"},\"quality\":{\"id\":\"TAO0001517\",\"name\":\"fusedwith\"},\"related_entity\":{\"id\":\"TAO0001518\",\"name\":\"posteriorceratohyal\"}},{\"gene\":{\"id\":\"TAO0001522\",\"name\":\"brph2\"},\"entity\":{\"id\":\"TAO0001524\",\"name\":\"fossaofbasihyalbone\"},\"quality\":{\"id\":\"TAO0001525\",\"name\":\"decreasedwidth\"},\"related_entity\":null}]}");
        } catch (JSONException e) {
            return null;
        }                
    }

}
