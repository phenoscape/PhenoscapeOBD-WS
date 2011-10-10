package org.phenoscape.obd.query;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;

public class DistinctPhenotypesCountSolrQuery {

    private final SolrServer solr;
    private final String entityID;
    private final String qualityID;
    private final String relatedEntityID;
    private final String taxonID;
    private final String geneID;

    public DistinctPhenotypesCountSolrQuery(SolrServer solr, String entityID, String qualityID, String relatedEntityID, String taxonID, String geneID) {
        this.solr = solr;
        this.entityID = entityID;
        this.qualityID = qualityID;
        this.relatedEntityID = relatedEntityID;
        this.taxonID = taxonID;
        this.geneID = geneID;
    }

    public int getCount() throws SolrServerException {
        final SolrQuery query = new SolrQuery();
        final List<String> facets = new ArrayList<String>();
        facets.add("type:\"phenotype\"");
        if (this.taxonID != null) {
            facets.add(String.format("taxon_asserted:\"%s\"", this.taxonID));
        }
        if (this.entityID != null) {
            facets.add(String.format("entity_strict_inheres_in:\"%s\"", this.entityID));
        }
        if (this.qualityID != null) {
            facets.add(String.format("quality:\"%s\"", this.qualityID));
        }
        if (this.relatedEntityID != null) {
            facets.add(String.format("related_entity:\"%s\"", this.relatedEntityID));
        }
        if (this.geneID != null) {
            facets.add(String.format("gene:\"%s\"", this.geneID));
        }
        query.setQuery(StringUtils.join(facets, " AND "));
        query.setRows(0);
        final QueryResponse response = this.solr.query(query);
        return new Long(response.getResults().getNumFound()).intValue();
    }

    @SuppressWarnings("unused")
    private Logger log() {
        return Logger.getLogger(this.getClass());
    }

}
