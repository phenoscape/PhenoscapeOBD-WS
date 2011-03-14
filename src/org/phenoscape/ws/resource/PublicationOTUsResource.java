package org.phenoscape.ws.resource;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.model.OTU;
import org.phenoscape.obd.model.Specimen;
import org.restlet.data.Reference;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public class PublicationOTUsResource extends AbstractPhenoscapeResource {

    private String publicationID = null;

    @Override
    protected void doInit() throws ResourceException {
        super.doInit();
        this.publicationID = Reference.decode((String)(this.getRequestAttributes().get("publicationID")));
    }

    @Get("json")
    public Representation getJSONRepresentation() {
        try {
            final List<OTU> otus = this.getDataStore().getOTUsForPublication(this.publicationID);
            Collections.sort(otus, otuComparator);
            final JSONObject json = this.translate(otus);
            return new JsonRepresentation(json);
        } catch (SQLException e) {
            log().error("Error querying database", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        } catch (JSONException e) {
            log().error("Error creating JSON document", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        }
    }

    private JSONObject translate(List<OTU> otus) throws JSONException {
        final JSONObject json = new JSONObject();
        final List<JSONObject> jsonOTUs = new ArrayList<JSONObject>();
        for (OTU otu : otus) {
            final JSONObject jsonOTU = new JSONObject();
            jsonOTU.put("label", otu.getLabel());
            jsonOTU.put("comment", otu.getComment());
            log().debug("Taxon: " + otu.getTaxon());
            jsonOTU.put("taxon", TermResourceUtil.translateMinimal(otu.getTaxon()));
            final List<JSONObject> jsonSpecimens = new ArrayList<JSONObject>();
            for (Specimen specimen : otu.getSpecimens()) {
                final JSONObject jsonSpecimen = new JSONObject();
                jsonSpecimen.put("collection", TermResourceUtil.translateMinimal(specimen.getCollection()));
                jsonSpecimen.put("catalog_number", specimen.getCatalogNumber());
                jsonSpecimens.add(jsonSpecimen);
            }
            jsonOTU.put("specimens", jsonSpecimens);
            jsonOTUs.add(jsonOTU);
        }
        json.put("otus", jsonOTUs);
        return json;
    }

    private static final Comparator<OTU> otuComparator = new Comparator<OTU>() {

        @Override
        public int compare(OTU o1, OTU o2) {
            return o1.getLabel().compareTo(o2.getLabel());
        }

    };

}
