package org.phenoscape.ws.resource;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.model.PhenotypeSpec;
import org.phenoscape.obd.model.TaxonAnnotation;
import org.phenoscape.obd.query.AnnotationsQueryConfig;
import org.phenoscape.obd.query.PhenoscapeDataStore.POSTCOMP_OPTION;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public class TaxonAnnotationSourceResource extends AbstractPhenoscapeResource {

    private AnnotationsQueryConfig config;
    private POSTCOMP_OPTION postcompOption = POSTCOMP_OPTION.SIMPLE_LABEL;
    private static final Map<String,POSTCOMP_OPTION> POSTCOMP_OPTIONS = new HashMap<String,POSTCOMP_OPTION>();
    static {
        POSTCOMP_OPTIONS.put("structure", POSTCOMP_OPTION.STRUCTURE);
        POSTCOMP_OPTIONS.put("semantic", POSTCOMP_OPTION.SEMANTIC_LABEL);
        POSTCOMP_OPTIONS.put("simple", POSTCOMP_OPTION.SIMPLE_LABEL);
        POSTCOMP_OPTIONS.put("none", POSTCOMP_OPTION.NONE);
    }

    @Override
    protected void doInit() throws ResourceException {
        super.doInit();
        final String pcOption = this.getFirstQueryValue("postcompositions");
        if (pcOption != null) {
            if (POSTCOMP_OPTIONS.containsKey(pcOption)) {
                this.postcompOption = POSTCOMP_OPTIONS.get(pcOption);
            } else {
                throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, "Invalid postcomposition option");
            }
        }
        try {
            this.config = this.initializeQueryConfig(this.getJSONQueryValue("query", new JSONObject()));
            this.config.setPostcompositionOption(this.postcompOption);
            this.validateConfig(this.config);
        } catch (JSONException e) {
            throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, e);
        }
        
    }

    @Get("json")
    public Representation getJSONRepresentation() {
        try {
            final List<TaxonAnnotation> annotations = this.getDataStore().getSupportingTaxonomicAnnotations(this.config);
            return new JsonRepresentation(this.convertToJSON(annotations));
        } catch (JSONException e) {
            log().error("Failed to create JSON object for annotations", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        } catch (SQLException e) {
            log().error("Database error querying for annotations", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        }
    }

    private void validateConfig(AnnotationsQueryConfig queryConfig) throws ResourceException {
        if (config.getTaxonIDs().size() != 1) {
            throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST);
        }
        if (config.getPhenotypes().size() != 1) {
            throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST);
        } else {
            final PhenotypeSpec phenotype = config.getPhenotypes().get(0);
            if (phenotype.getEntityID() == null) {
                throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST);
            }
            if (phenotype.getQualityID() == null) {
                throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST);
            }
        }
    }

    private JSONObject convertToJSON(List<TaxonAnnotation> annotations) throws JSONException {
        final JSONObject json = new JSONObject();
        final JSONArray array = new JSONArray();
        for (TaxonAnnotation taxonAnnotation : annotations) {
            array.put(this.convertToJSON(taxonAnnotation));
        }
        json.put("annotations", array);
        return json;
    }

    private JSONObject convertToJSON(TaxonAnnotation taxonAnnotation) throws JSONException {
        final JSONObject json = new JSONObject();
        final JSONObject taxon = this.createBasicJSONTerm(taxonAnnotation.getTaxon());
        taxon.put("extinct", taxonAnnotation.getTaxon().isExtinct());
        if (taxonAnnotation.getTaxon().getRank() != null) {
            final JSONObject rank = this.createBasicJSONTerm(taxonAnnotation.getTaxon().getRank());
            taxon.put("rank", rank);
        }
        json.put("taxon", taxon);
        final JSONObject entity = this.createBasicJSONTerm(taxonAnnotation.getEntity());
        json.put("entity", entity);
        final JSONObject quality = this.createBasicJSONTerm(taxonAnnotation.getQuality());
        json.put("quality", quality);
        if (taxonAnnotation.getRelatedEntity() != null) {
            final JSONObject relatedEntity = this.createBasicJSONTerm(taxonAnnotation.getRelatedEntity());
            json.put("related_entity", relatedEntity);
        }
        final JSONObject publication = this.createBasicJSONTerm(taxonAnnotation.getPublication());
        json.put("publication", publication);
        final JSONObject character = new JSONObject();
        character.put("text", taxonAnnotation.getCharacter().getLabel());
        character.put("character_number", taxonAnnotation.getCharacter().getNumber());
        json.put("character", character);
        final JSONObject state = new JSONObject();
        state.put("text", taxonAnnotation.getState().getLabel());
        json.put("state", state);
        json.put("curator", taxonAnnotation.getCurator());
        json.put("otu", taxonAnnotation.getOtu().getLabel());
        return json;    
    }

}
