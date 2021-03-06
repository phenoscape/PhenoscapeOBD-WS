package org.phenoscape.obd.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Transformer;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.phenoscape.obd.model.PhenotypeSpec;
import org.phenoscape.obd.query.AnnotationsQueryConfig.SORT_COLUMN;

public class PublicationsSolrQuery {

    private final SolrServer solr;
    private final AnnotationsQueryConfig config;
    private static final Map<SORT_COLUMN, String> COLUMNS = new HashMap<SORT_COLUMN, String>();
    static {
        COLUMNS.put(SORT_COLUMN.PUBLICATION, "label");
    }

    public PublicationsSolrQuery(SolrServer solr, AnnotationsQueryConfig config) {
        this.solr = solr;
        this.config = config;
    }

    public QueryResponse executeQuery() throws SolrServerException {
        final SolrQuery query = new SolrQuery();
        query.setQuery(this.getMainQuery());
        query.setRows(this.config.getLimit());
        query.setStart(this.config.getIndex());
        query.setSortField(COLUMNS.get(this.config.getSortColumn()), (this.config.sortDescending() ? ORDER.desc : ORDER.asc));
        if (!this.config.getPhenotypes().isEmpty()) {
            this.addPhenotypeFilter(query);
        }
        query.setFields("id", "label");
        log().debug(query.toString());
        return this.solr.query(query);
    }

    private String getMainQuery() {
        final StringBuffer query = new StringBuffer();
        query.append("type:\"publication\"");
        if (!this.config.getTaxonIDs().isEmpty()) {
            query.append(" AND taxon:");
            query.append("(");
            query.append(StringUtils.join(CollectionUtils.collect(this.config.getTaxonIDs(), quoter), (this.config.matchAllTaxa() ? " AND " : " OR ")));
            query.append(")");
        }
        return query.toString();
    }

    private void addPhenotypeFilter(SolrQuery query) {
        if (this.config.matchAllPhenotypes()) {
            for (PhenotypeSpec phenotype : this.config.getPhenotypes()) {
                final StringBuffer buffer = new StringBuffer();
                buffer.append("{!join from=id to=phenotype}");
                buffer.append(phenotypeTransformer.transform(phenotype));
                query.addFilterQuery(buffer.toString());
            }
        } else {
            final StringBuffer buffer = new StringBuffer();
            buffer.append("{!join from=id to=phenotype}");
            buffer.append("(");
            buffer.append(StringUtils.join(CollectionUtils.collect(this.config.getPhenotypes(), phenotypeTransformer), " OR "));
            buffer.append(")");
            query.addFilterQuery(buffer.toString());
        }       
    }

    private static Transformer phenotypeTransformer = new Transformer() {

        @Override
        public Object transform(Object item) {
            final PhenotypeSpec phenotype = (PhenotypeSpec)item;
            final List<String> components = new ArrayList<String>();
            if (phenotype.getEntityID() != null) {
                final String field = phenotype.includeEntityParts() ? "entity" : "entity_strict_inheres_in";
                components.add(String.format("%s:\"%s\"", field, phenotype.getEntityID()));
            }
            if (phenotype.getQualityID() != null) {
                components.add(String.format("quality:\"%s\"", phenotype.getQualityID()));
            }
            if (phenotype.getRelatedEntityID() != null) {
                components.add(String.format("related_entity:\"%s\"", phenotype.getRelatedEntityID()));
            }
            return "(" + StringUtils.join(components, " AND ") + ")";
        }

    };


    private static Transformer quoter = new Transformer() {

        @Override
        public Object transform(Object taxonID) {
            return String.format("\"%s\"", taxonID);
        }

    };

    private Logger log() {
        return Logger.getLogger(this.getClass());
    }



}
