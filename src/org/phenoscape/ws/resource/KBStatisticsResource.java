package org.phenoscape.ws.resource;

import java.sql.SQLException;

import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.query.GeneAnnotationsQueryConfig;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;

public class KBStatisticsResource extends AbstractPhenoscapeResource {

    @Get("json")
    public Representation getJSONRepresentation() {
        try {
            return new JsonRepresentation(this.queryStatistics());
        } catch (JSONException e) {
            log().error("Error creating JSON object for data", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        } catch (SQLException e) {
            log().error("Database error querying for timestamp", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        }
    }

    private JSONObject queryStatistics() throws JSONException, SQLException {
        //e.g. The Knowledgebase currently contains 333,987 phenotype statements about 2310 taxa, 
        //sourced from 51 publications. as well as 11,267 phenotype statements about 2953 genes
        final JSONObject json = new JSONObject();
        final int genesCount = this.getDataStore().getCountOfAnnotatedGenes();
        final int geneAnnotationsCount = this.getDataStore().getCountOfGeneAnnotations(new GeneAnnotationsQueryConfig());
        final int taxaCount = this.getDataStore().getCountOfAnnotatedTaxa(false);
        final int taxonAnnotationsCount = this.getDataStore().getCountOfTaxonomicAnnotations(false);
        json.put("annotated_genes", genesCount);
        json.put("gene_annotations", geneAnnotationsCount);
        json.put("annotated_taxa", taxaCount);
        json.put("taxon_annotations", taxonAnnotationsCount);
        return json;
    }

}
