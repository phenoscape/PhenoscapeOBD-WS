package org.phenoscape.ws.resource;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.solr.client.solrj.SolrServerException;
import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.query.EntityFaceter;
import org.phenoscape.obd.query.QualityFaceter;
import org.restlet.data.Reference;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public class PhenotypesFacetResource extends AbstractPhenoscapeResource {

    private final static int OPTIMAL_SIZE = 7;
    private static enum FACET { ENTITY, QUALITY, RELATED_ENTITY, TAXON, GENE }
    private static final Map<String, FACET> uriToFacet = new HashMap<String, FACET>();
    static {
        uriToFacet.put("entity", FACET.ENTITY);
        uriToFacet.put("quality", FACET.QUALITY);
        uriToFacet.put("related_entity", FACET.RELATED_ENTITY);
        uriToFacet.put("taxon", FACET.TAXON);
        uriToFacet.put("gene", FACET.GENE);
    }
    private FACET facet;
    private List<String> facetPath;
    private String entityID;
    private String qualityID;
    private String relatedEntityID;
    private String taxonID;
    private String geneID;

    @Override
    protected void doInit() throws ResourceException {
        super.doInit();
        final String requestedFacet = Reference.decode((String)(this.getRequestAttributes().get("facet")));
        if (uriToFacet.containsKey(requestedFacet)) {
            this.facet = uriToFacet.get(requestedFacet);
        } else {
            throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, "Invalid facet requested");
        }
        if (this.getQuery().contains(requestedFacet)) {
            throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, "Should not provide query parameter for requested facet");
        }
        this.entityID = this.getFirstQueryValue("entity");
        this.qualityID = this.getFirstQueryValue("quality");
        this.relatedEntityID = this.getFirstQueryValue("related_entity");
        this.taxonID = this.getFirstQueryValue("taxon");
        this.geneID = this.getFirstQueryValue("gene");
        final String requestedPath = this.getFirstQueryValue("path");
        if (requestedPath != null) {
            this.facetPath = this.parsePathParameter(requestedPath);
        } else {
            this.facetPath = Collections.emptyList();
        }
    }

    @Get("json")
    public Representation getJSONRepresentation() throws JSONException {
        try {
            final JSONObject json = new JSONObject();
            final List<JSONObject> pathItems = new ArrayList<JSONObject>();
            final JSONObject any = new JSONObject();
            any.put("count", this.getPhenotypeCount(null));
            pathItems.add(any);
            for (String termID : this.facetPath) {
                final JSONObject current = new JSONObject();
                current.put("id", termID);
                current.put("count", this.getPhenotypeCount(termID));
                pathItems.add(current);
            }
            final Map<String, Integer> childrenCounts = this.facetPath.isEmpty() ? this.getFacetedPhenotypeCount(null) : this.getFacetedPhenotypeCount(this.facetPath.get(this.facetPath.size() - 1));
            final List<JSONObject> children = new ArrayList<JSONObject>();
            for (Entry<String, Integer> entry : childrenCounts.entrySet()) {
                final JSONObject child = new JSONObject();
                child.put("id", entry.getKey());
                child.put("count", entry.getValue());
                children.add(child);
            }
            pathItems.get(pathItems.size() - 1).put("children", children);
            json.put("facet", pathItems);
            return new JsonRepresentation(json);
        }
        catch (JSONException e) {
            log().error("Failed to create JSON object for results", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        } catch (SolrServerException e) {
            log().error("Error querying Solr server", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        } catch (SQLException e) {
            log().error("Error querying database", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            e.printStackTrace();
            return null;
        } 
    }

    private int getPhenotypeCount(String focalTermID) throws SolrServerException {
        final int count;
        switch(this.facet) {
        case ENTITY: count = this.getDataStore().getCountOfDistinctPhenotypes(focalTermID, this.qualityID, this.relatedEntityID, this.taxonID, this.geneID); break;
        case QUALITY: count = this.getDataStore().getCountOfDistinctPhenotypes(this.entityID, focalTermID, this.relatedEntityID, this.taxonID, this.geneID); break;
        case RELATED_ENTITY: count = this.getDataStore().getCountOfDistinctPhenotypes(this.entityID, this.qualityID, focalTermID, this.taxonID, this.geneID); break;
        case TAXON: count = this.getDataStore().getCountOfDistinctPhenotypes(this.entityID, this.qualityID, this.relatedEntityID, focalTermID, this.geneID); break;
        case GENE: count = this.getDataStore().getCountOfDistinctPhenotypes(this.entityID, this.qualityID, this.relatedEntityID, this.taxonID, focalTermID); break;
        default: count = -1; //should never happen
        }
        return count;
    }
    
    private Map<String, Integer> getFacetedPhenotypeCount(String focalTermID) throws SQLException, SolrServerException {
        final Map<String, Integer> counts;
        switch(this.facet) {
        case ENTITY: counts = (new EntityFaceter(this.getDataStore(), OPTIMAL_SIZE) {
            @Override
            protected int getDataCount(String focalTermUID) throws SolrServerException {
                return this.getDataStore().getCountOfDistinctPhenotypes(focalTermUID, qualityID, relatedEntityID, taxonID, geneID);
            }
        }).facetTerm(focalTermID); break;
        case QUALITY: counts = (new QualityFaceter(this.getDataStore(), OPTIMAL_SIZE) {
            @Override
            protected int getDataCount(String focalTermUID) throws SolrServerException {
                return this.getDataStore().getCountOfDistinctPhenotypes(entityID, focalTermUID, relatedEntityID, taxonID, geneID);
            }
        }).facetTerm(focalTermID); break; 
            
//        case RELATED_ENTITY: counts = this.getDataStore().getCountOfDistinctPhenotypes(this.entityID, this.qualityID, focalTermID, this.taxonID, this.geneID); break;
//        case TAXON: counts = this.getDataStore().getCountOfDistinctPhenotypes(this.entityID, this.qualityID, this.relatedEntityID, focalTermID, this.geneID); break;
//        case GENE: counts = this.getDataStore().getCountOfDistinctPhenotypes(this.entityID, this.qualityID, this.relatedEntityID, this.taxonID, focalTermID); break;
        default: counts = null; //should never happen
        }
        return counts;
    }

    private List<String> parsePathParameter(String pathParameter) {
        return Arrays.asList(pathParameter.split(","));
    }

}
