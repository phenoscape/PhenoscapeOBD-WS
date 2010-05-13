package org.obd.ws.resources;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.query.OBDQuery;
import org.phenoscape.util.PhenotypeAndAnnotatedSubtaxonCountDTO;
import org.phenoscape.util.Queries;
import org.phenoscape.ws.application.PhenoscapeWebServiceApplication;
import org.restlet.data.Reference;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public class SquarifiedTaxonMapResource extends AbstractOBDResource {

    private String taxonID;
    private OBDQuery obdq;
    private Queries queries;

    @Override
    protected void doInit() throws ResourceException {
        super.doInit();
        this.taxonID = Reference.decode((String)(this.getRequestAttributes().get("taxonID")));
        this.queries = (Queries)this.getContext().getAttributes().get(PhenoscapeWebServiceApplication.QUERIES_STRING);
    }

    @Get("json")
    public Representation getJSONRepresentation() throws ResourceException {
        try {
            this.connectShardToDatabase();
            if(!inputFormParametersAreValid()){
                this.disconnectShardFromDatabase();
                return null;
            }
            obdq = new OBDQuery(this.getShard(), queries);
            final JSONObject json = this.assembleJSONObject(taxonID);
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
        } catch (JSONException e) {
            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, "[JSON EXCEPTION] Error with JSON Object");
        } finally {
            disconnectShardFromDatabase();
        }
        return null;
    }

    private boolean inputFormParametersAreValid(){
        if (taxonID == null) {
            getResponse().setStatus(
                    Status.CLIENT_ERROR_BAD_REQUEST,
                    "ERROR: The input parameter for taxon "
                    + "is null");
            return false;
        }
        return true;
    }

    protected JSONObject  assembleJSONObject(String taxonUID) throws SQLException, JSONException {

        JSONObject searchObj, dataObj, childObj;
        List<JSONObject> childObjs = new ArrayList<JSONObject>();
        List<JSONObject> childrenOfChildObj = new ArrayList<JSONObject>();

        List<PhenotypeAndAnnotatedSubtaxonCountDTO> results = obdq.executeQueryForSquarifiedTaxonMapResource(taxonID);

        PhenotypeAndAnnotatedSubtaxonCountDTO searchTaxonObject = results.remove(0);
        final String id = searchTaxonObject.getId();
        final String name = searchTaxonObject.getName();
        final int phenotypeCount = searchTaxonObject.getPhenotypeCount();
        final double subtaxonCount;

        if(searchTaxonObject.getSubtaxonCount() > 1)
            subtaxonCount = Math.log(searchTaxonObject.getSubtaxonCount());//getting log values to reduce scales of comparison
        else
            subtaxonCount = searchTaxonObject.getSubtaxonCount();

        searchObj = new JSONObject();
        searchObj.put("id", id);
        searchObj.put("name", name);

        dataObj = new JSONObject();
        dataObj.put("$area", subtaxonCount);
        dataObj.put("$color", phenotypeCount);

        searchObj.put("data", dataObj);

        for (PhenotypeAndAnnotatedSubtaxonCountDTO child : results) {
            childObj = new JSONObject();

            final String childID = child.getId();
            final String childName = child.getName();
            final int childPhenotypeCount = child.getPhenotypeCount(); 
            final double childSubtaxonCount;

            if(child.getSubtaxonCount() > 1)
                childSubtaxonCount = Math.log(child.getSubtaxonCount());//getting log values to reduce scales of comparison
            else
                childSubtaxonCount = child.getSubtaxonCount();

            childObj.put("id", childID);
            childObj.put("name", childName);

            dataObj = new JSONObject();
            dataObj.put("$area", childSubtaxonCount);
            dataObj.put("$color", (((childPhenotypeCount*1.0)/phenotypeCount)*100));

            childObj.put("data", dataObj);
            childObj.put("children", childrenOfChildObj);
            childObjs.add(childObj);
        }

        searchObj.put("children", childObjs);

        return searchObj;
    }

}
