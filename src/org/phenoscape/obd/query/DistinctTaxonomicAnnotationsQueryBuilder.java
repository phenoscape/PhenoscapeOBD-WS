package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.phenoscape.obd.model.PhenotypeSpec;
import org.phenoscape.obd.model.Vocab.OBO;
import org.phenoscape.obd.query.TaxonAnnotationsQueryConfig.SORT_COLUMN;

public class DistinctTaxonomicAnnotationsQueryBuilder extends QueryBuilder {

    private final TaxonAnnotationsQueryConfig config;
    private final boolean totalOnly;
    private static final String TABLE = "queryable_taxon_annotation";
    private static final String SELECT = "SELECT DISTINCT queryable_taxon_annotation.taxon_node_id, queryable_taxon_annotation.taxon_uid, queryable_taxon_annotation.taxon_label, queryable_taxon_annotation.taxon_rank_node_id, queryable_taxon_annotation.taxon_rank_uid, queryable_taxon_annotation.taxon_rank_label, queryable_taxon_annotation.taxon_is_extinct, queryable_taxon_annotation.phenotype_node_id, queryable_taxon_annotation.phenotype_uid, queryable_taxon_annotation.phenotype_label, queryable_taxon_annotation.entity_node_id, queryable_taxon_annotation.entity_uid, queryable_taxon_annotation.entity_label, queryable_taxon_annotation.quality_node_id, queryable_taxon_annotation.quality_uid, queryable_taxon_annotation.quality_label, queryable_taxon_annotation.related_entity_node_id, queryable_taxon_annotation.related_entity_uid, queryable_taxon_annotation.related_entity_label FROM queryable_taxon_annotation ";
    private static final Map<SORT_COLUMN, String> COLUMNS = new HashMap<SORT_COLUMN, String>();
    static {
        COLUMNS.put(SORT_COLUMN.TAXON, "taxon_label");
        COLUMNS.put(SORT_COLUMN.ENTITY, "entity_label");
        COLUMNS.put(SORT_COLUMN.QUALITY, "quality_label");
        COLUMNS.put(SORT_COLUMN.RELATED_ENTITY, "related_entity_label");
    }

    public DistinctTaxonomicAnnotationsQueryBuilder(TaxonAnnotationsQueryConfig config, boolean totalOnly) {
        this.config = config;
        this.totalOnly = totalOnly;
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        int index = 1;
        for (String taxonID : this.config.getTaxonIDs()) {
            statement.setString(index++, taxonID);
        }
        if (!this.config.getPhenotypes().isEmpty()) {
            for (PhenotypeSpec phenotype : this.config.getPhenotypes()) {
                if (phenotype.getEntityID() != null) {
                    statement.setString(index++, phenotype.getEntityID());                    
                }
                if (phenotype.getQualityID() != null) {
                    statement.setString(index++, phenotype.getQualityID());
                }
                if (phenotype.getRelatedEntityID() != null) {
                    statement.setString(index++, phenotype.getRelatedEntityID());                    
                }
            }
        }
        if (!this.config.getPublicationIDs().isEmpty()) {
            for (String publicationID : this.config.getPublicationIDs()) {
                statement.setString(index++, publicationID);
            }
        }
        if (!this.totalOnly) {
            statement.setInt(index++, this.config.getLimit());
            statement.setInt(index++, this.config.getIndex());
        }
    }

    @Override
    protected String getQuery() {
        final List<String> intersects = new ArrayList<String>();
        if (!this.config.getTaxonIDs().isEmpty()) {
            intersects.add(this.getTaxaQuery(this.config.getTaxonIDs()));
        }
        if (!this.config.getPhenotypes().isEmpty()) {
            intersects.add(this.getPhenotypesQuery(this.config.getPhenotypes()));
        }
        if (!this.config.getPublicationIDs().isEmpty()) {
            intersects.add(this.getPublicationsQuery(this.config.getPublicationIDs()));
        }
        final String baseQuery;
        if (intersects.isEmpty()) {
            baseQuery = SELECT + (this.config.includeInferredAnnotations() ? "" : "WHERE queryable_taxon_annotation.is_inferred = false ");
        } else {
            baseQuery = "(" + StringUtils.join(intersects, " INTERSECT ") + ") ";
        }
        final String query;
        if (this.totalOnly) {
            query = "SELECT count(*) FROM (" + baseQuery + ") AS query";
        } else {
            query = baseQuery + "ORDER BY " + COLUMNS.get(this.config.getSortColumn()) + " " + this.getSortText() + "LIMIT ? OFFSET ? " ;
        }
        log().debug("Query: " + query);
        return query;
    }

    private String getSortText() {
        return this.config.sortDescending() ? "DESC " : ""; 
    }

    private String getTaxaQuery(List<String> taxonIDs) {
        final StringBuffer query = new StringBuffer();
        query.append("(");
        final List<String> unions = new ArrayList<String>();
        for (String taxonID : taxonIDs) {
            unions.add(this.getTaxonQuery(taxonID));
        }
        query.append(StringUtils.join(unions, " UNION "));
        query.append(")");
        return query.toString();
    }

    private String getTaxonQuery(String taxonID) {
        final StringBuffer query = new StringBuffer();
        query.append("(");
        query.append(SELECT);
        query.append(String.format("JOIN link taxon_is_a ON (taxon_is_a.node_id = %s.taxon_node_id AND taxon_is_a.predicate_id = %s AND taxon_is_a.object_id = %s) ", TABLE, this.node(OBO.IS_A), NODE));
        query.append(this.config.includeInferredAnnotations() ? "" : " WHERE queryable_taxon_annotation.is_inferred = false ");
        query.append(") ");
        return query.toString();
    }

    private String getPhenotypesQuery(List<PhenotypeSpec> phenotypes) {
        final StringBuffer query = new StringBuffer();
        query.append("(");
        final List<String> unions = new ArrayList<String>();
        for (PhenotypeSpec phenotype : phenotypes) {
            unions.add(this.getPhenotypeQuery(phenotype));
        }
        query.append(StringUtils.join(unions, " UNION "));
        query.append(")");
        return query.toString();
    }

    private String getPhenotypeQuery(PhenotypeSpec phenotype) {
        final StringBuffer query = new StringBuffer();
        query.append("(");
        query.append(SELECT);
        if (phenotype.getEntityID() != null) {
            if (phenotype.includeEntityParts()) {
                query.append(String.format("JOIN link phenotype_inheres_in_part_of ON (phenotype_inheres_in_part_of.node_id = %s.phenotype_node_id AND phenotype_inheres_in_part_of.predicate_id = %s) ", TABLE, this.node(OBO.INHERES_IN_PART_OF)));    
            } else {
                query.append(String.format("JOIN link phenotype_inheres_in ON (phenotype_inheres_in.node_id = %s.phenotype_node_id AND phenotype_inheres_in.predicate_id = %s) ", TABLE, this.node(OBO.INHERES_IN)));
            }
        }
        if (phenotype.getQualityID() != null) {
            query.append(String.format("JOIN link quality_is_a ON (quality_is_a.node_id = %s.quality_node_id AND quality_is_a.predicate_id = %s) ", TABLE, this.node(OBO.IS_A)));
        }
        if (phenotype.getRelatedEntityID() != null) {
            query.append(String.format("JOIN link related_entity_is_a ON (related_entity_is_a.node_id = %s.related_entity_node_id AND related_entity_is_a.predicate_id = %s) ", TABLE, this.node(OBO.IS_A)));  
        }
        query.append("WHERE ");
        query.append(this.translate(phenotype));
        query.append(this.config.includeInferredAnnotations() ? "" : " AND queryable_taxon_annotation.is_inferred = false ");
        query.append(") ");
        return query.toString();
    }

    private String translate(PhenotypeSpec phenotype) {
        final StringBuffer buffer = new StringBuffer();
        buffer.append("(");
        final List<String> terms = new ArrayList<String>();
        if (phenotype.getEntityID() != null) {
            if (phenotype.includeEntityParts()) {
                terms.add("phenotype_inheres_in_part_of.object_id = " + NODE + " ");    
            } else {
                terms.add("phenotype_inheres_in.object_id = " + NODE + " ");
            }
        }
        if (phenotype.getQualityID() != null) {
            terms.add("quality_is_a.object_id = " + NODE + " ");
        }
        if (phenotype.getRelatedEntityID() != null) {
            terms.add("related_entity_is_a.object_id = " + NODE + " ");
        }
        buffer.append(StringUtils.join(terms, " AND "));
        buffer.append(")");
        return buffer.toString();
    }

    private String getPublicationsQuery(List<String> publicationIDs) {
        final StringBuffer query = new StringBuffer();
        query.append("(");
        query.append(SELECT);
        query.append("WHERE ");
        query.append("publication_uid IN ");
        query.append(this.createPlaceholdersList(publicationIDs.size()));
        query.append(this.config.includeInferredAnnotations() ? "" : " AND queryable_taxon_annotation.is_inferred = false ");
        query.append(") ");
        return query.toString();
    }

    private String node(String uid) {
        return String.format(NODE_S, uid);
    }

    private Logger log() {
        return Logger.getLogger(this.getClass());
    }

}
