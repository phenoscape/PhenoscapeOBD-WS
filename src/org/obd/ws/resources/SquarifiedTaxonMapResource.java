package org.obd.ws.resources;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;
import org.obd.ws.application.OBDApplication;
import org.obd.ws.util.Queries;
import org.obd.ws.util.dto.PhenotypeAndAnnotatedSubtaxonCountDTO;
import org.phenoscape.obd.OBDQuery;
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
    private OBDQuery obdq;
  
    private Queries queries;
    
    public SquarifiedTaxonMapResource(Context context, Request request, Response response) throws SQLException, ClassNotFoundException {
        super(context, request, response);
        this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
        this.taxonID = Reference.decode((String)(request.getAttributes().get("taxonID")));
        this.queries = (Queries)this.getContext().getAttributes().get(OBDApplication.QUERIES_STRING);
    }

    @Override
    public Representation represent(Variant variant) throws ResourceException {
        try {
            this.connectShardToDatabase();
            if(!inputFormParametersAreValid()){
				this.disconnectShardFromDatabase();
				return null;
			}
            obdq = new OBDQuery(shard, queries);
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
    	String id, name, phenotypeCount, subtaxonCount;
    	
    	List<PhenotypeAndAnnotatedSubtaxonCountDTO> results =
    						obdq.executeQueryForSquarifiedTaxonMapResource(taxonID);
    	
    	PhenotypeAndAnnotatedSubtaxonCountDTO searchTaxonObject = results.remove(0);
    	id = searchTaxonObject.getId();
    	name = searchTaxonObject.getName();
    	phenotypeCount = searchTaxonObject.getPhenotypeCount() + "";
    	subtaxonCount = Math.log(searchTaxonObject.getSubtaxonCount()) + "";
    	    	
    	searchObj = new JSONObject();
    	searchObj.put("id", id);
    	searchObj.put("name", name);
    	
    	dataObj = new JSONObject();
		dataObj.put("$area", subtaxonCount);
		dataObj.put("annotations", phenotypeCount);
    	
    	searchObj.put("data", dataObj);
    	
    	for(PhenotypeAndAnnotatedSubtaxonCountDTO child : results){
    		childObj = new JSONObject();
    		
    		id = child.getId();
    		name = child.getName();
    		phenotypeCount = child.getPhenotypeCount() + ""; 
    		subtaxonCount = Math.log(child.getSubtaxonCount()) + "";//getting log values to reduce scales of comparison
    		
    		childObj.put("id", id);
    		childObj.put("name", name);
    		
    		dataObj = new JSONObject();
    		dataObj.put("$area", subtaxonCount);
    		dataObj.put("annotations", phenotypeCount);
    		
    		childObj.put("data", dataObj);
    		childObj.put("children", childrenOfChildObj);
    		childObjs.add(childObj);
    	}
    	
    	searchObj.put("children", childObjs);
    	
    	return searchObj;
    }

}
