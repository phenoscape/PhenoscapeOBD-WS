package org.phenoscape.ws.resource;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.model.Phenotype;
import org.phenoscape.obd.model.PhenotypeVariationSet;
import org.phenoscape.obd.model.Vocab.TTO;
import org.phenoscape.obd.query.AnnotationsQueryConfig;
import org.phenoscape.obd.query.QueryException;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public class PhenotypeVariationResource extends AbstractPhenoscapeResource {

    private String taxonID;
    private boolean excludeGivenQuality;
    private boolean excludeUnannotatedTaxa;
    private AnnotationsQueryConfig phenotype;

    @Override
    protected void doInit() throws ResourceException {
        super.doInit();
        this.taxonID = this.getFirstQueryValue("taxon");
        this.excludeGivenQuality = this.getBooleanQueryValue("exclude_attribute", true);
        this.excludeUnannotatedTaxa = this.getBooleanQueryValue("exclude_unannotated", true);
        try {
            this.phenotype = this.initializeQueryConfig(this.getJSONQueryValue("query", new JSONObject()));
            if (this.phenotype.getPhenotypes().size() != 1) {
                log().error("Only one phenotype should be included.");
                throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, "Only one phenotype should be included.");
            }
        } catch (QueryException e) {
            log().error("Incorrectly formatted phenotype query", e);
            throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, e);
        } catch (JSONException e) {
            log().error("Incorrectly formatted phenotype query", e);
            throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, e);
        }
    }

    @Get("json")
    public Representation getJSONRepresentation() {
        try {
            final Set<PhenotypeVariationSet> phenotypeSets;
            if (this.taxonID == null) {
                phenotypeSets = this.getDataStore().getPhenotypeSetsForChildren(TTO.ROOT, this.phenotype.getPhenotypes().get(0), true, this.excludeGivenQuality, this.excludeUnannotatedTaxa);
            } else {
                phenotypeSets = this.getDataStore().getPhenotypeSetsForChildren(taxonID, this.phenotype.getPhenotypes().get(0), false, this.excludeGivenQuality, this.excludeUnannotatedTaxa);
            }
            final JSONObject json = new JSONObject();
            final List<JSONObject> jsonSets = new ArrayList<JSONObject>();
            for (PhenotypeVariationSet variationSet : phenotypeSets) {
                jsonSets.add(this.translate(variationSet));
            }
            json.put("phenotype_sets", jsonSets);
            return new JsonRepresentation(json);
        } catch (SQLException e) {
            log().error("Database error querying for phenotype variation", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        } catch (JSONException e) {
            log().error("Failed to create JSON object for phenotype variation", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        }
    }

    private JSONObject translate(PhenotypeVariationSet variationSet) throws JSONException {
        final JSONObject json = new JSONObject();
        json.put("taxa", variationSet.getTaxa());
        final List<JSONObject> phenotypes = new ArrayList<JSONObject>();
        for (Phenotype phenotype : variationSet.getPhenotypes()) {
            phenotypes.add(this.translate(phenotype));
        }
        json.put("phenotypes", phenotypes);
        return json;
    }

    private JSONObject translate(Phenotype phenotype) throws JSONException {
        final JSONObject json = new JSONObject();
        final JSONObject entity = TermResourceUtil.translateMinimal(phenotype.getEntity());
        json.put("entity", entity);
        final JSONObject quality = TermResourceUtil.translateMinimal(phenotype.getQuality());
        json.put("quality", quality);
        if (phenotype.getRelatedEntity() != null) {
            final JSONObject relatedEntity = TermResourceUtil.translateMinimal(phenotype.getRelatedEntity());
            json.put("related_entity", relatedEntity);
        }
        return json;
    }
}