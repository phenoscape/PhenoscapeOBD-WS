package org.phenoscape.ws.resource;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.model.GeneAnnotation;
import org.phenoscape.obd.model.PhenotypeSpec;
import org.phenoscape.obd.query.AnnotationsQueryConfig;
import org.phenoscape.obd.query.PhenoscapeDataStore.POSTCOMP_OPTION;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public class GeneAnnotationsSourceResource extends AbstractPhenoscapeResource {

    private AnnotationsQueryConfig config;
    private POSTCOMP_OPTION postcompOption = POSTCOMP_OPTION.NONE;
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
            final List<GeneAnnotation> annotations = this.getDataStore().getSupportingGenotypeAnnotations(this.config);
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
        if (config.getGeneIDs().size() != 1) {
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

    private JSONObject convertToJSON(List<GeneAnnotation> annotations) throws JSONException {
        final JSONObject json = new JSONObject();
        final JSONArray array = new JSONArray();
        for (GeneAnnotation geneAnnotation : annotations) {
            array.put(this.convertToJSON(geneAnnotation));
        }
        json.put("annotations", array);
        return json;
    }

    private JSONObject convertToJSON(GeneAnnotation geneAnnotation) throws JSONException {
        final JSONObject json = new JSONObject();
        final JSONObject gene = this.createBasicJSONTerm(geneAnnotation.getGene());
        json.put("gene", gene);
        final JSONObject genotype = this.createBasicJSONTerm(geneAnnotation.getGenotype());
        final JSONObject genotypeClass = this.createBasicJSONTerm(geneAnnotation.getGenotypeClass());
        genotype.put("type", genotypeClass);
        json.put("genotype", genotype);
        final JSONObject entity = this.createBasicJSONTerm(geneAnnotation.getEntity());
        json.put("entity", entity);
        final JSONObject quality = this.createBasicJSONTerm(geneAnnotation.getQuality());
        json.put("quality", quality);
        if (geneAnnotation.getRelatedEntity() != null) {
            final JSONObject relatedEntity = this.createBasicJSONTerm(geneAnnotation.getRelatedEntity());
            json.put("related_entity", relatedEntity);
        }
        final JSONObject publication = this.createBasicJSONTerm(geneAnnotation.getPublication());
        json.put("publication", publication);
        return json;    
    }

}
