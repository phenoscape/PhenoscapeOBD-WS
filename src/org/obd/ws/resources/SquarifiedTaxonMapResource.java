package org.obd.ws.resources;

import java.sql.SQLException;

import org.json.JSONObject;
import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Reference;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.resource.Representation;
import org.restlet.resource.ResourceException;
import org.restlet.resource.Variant;

public class SquarifiedTaxonMapResource extends AbstractOBDResource {
    
    private final String taxonID;

    public SquarifiedTaxonMapResource(Context context, Request request, Response response) {
        super(context, request, response);
        this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
        this.taxonID = Reference.decode((String)(request.getAttributes().get("taxonID")));
    }

    @Override
    public Representation represent(Variant variant) throws ResourceException {
        try {
            this.connectShardToDatabase();
            final JSONObject json = new JSONObject();
            // the value for the "$area" key should be the 
            // result of getCountOfAnnotationsWithinTaxon(), instead of 42
            // the value for the "species" key should be the
            // number of taxa of rank species within the given taxon
            //
            // return children only one level deep - all children have an empty
            // children list
            
//            {id: "TTO:151", name: "Halecostomi", data:{"$area": 42, species: 1234}, children: [
//{"id":"TTO:10000323","name":"Pachycormidae", data:{"$area": 42, species: 1234}, children: []},
//{"id":"TTO:1220", "name":"Amiiformes", data:{"$area": 42, species: 1234}, children: []},
//{"id":"TTO:10000305","name":"Aspidorhynchiformes", data:{"$area": 42, species: 1234}, children: []},
//{"id":"TTO:201","name":"Teleostei", data:{"$area": 42, species: 1234}, children: []},
// {"id":"TTO:10000290","name":"Parasemionotidae", data:{"$area": 42, species: 1234}, children: []}
//]}
            
            

            return new JsonRepresentation(json);
        } catch (SQLException sqle) {
            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, "[SQL EXCEPTION] Something broke server side. Consult server logs");
        } catch (ClassNotFoundException e) {
            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, "[CLASS NOT FOUND EXCEPTION] Something broke server side. Consult server logs");
        } finally {
            disconnectShardFromDatabase();
        }
        return null;
    }
    
    private int getCountOfAnnotationsWithinTaxon(String taxonUID) {
        //TODO get the real number
        return 42;
    }

}
