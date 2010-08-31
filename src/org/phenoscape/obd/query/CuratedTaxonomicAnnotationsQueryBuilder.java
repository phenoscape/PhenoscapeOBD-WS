package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.phenoscape.obd.model.PhenotypeSpec;
import org.phenoscape.obd.model.Vocab.OBO;

public class CuratedTaxonomicAnnotationsQueryBuilder extends QueryBuilder {

    private final AnnotationsQueryConfig config;
    private final boolean totalOnly;

    public CuratedTaxonomicAnnotationsQueryBuilder(AnnotationsQueryConfig config, boolean totalOnly) {
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
        final String baseQuery;
        if (intersects.isEmpty()) {
            if (this.totalOnly) {
                baseQuery = "SELECT * FROM annotation_source ";   
            } else {
                baseQuery = "SELECT annotation_source.*, taxon.node_id AS taxon_node_id, taxon.uid AS taxon_uid, taxon.label AS taxon_label, taxon.is_extinct AS taxon_is_extinct, taxon.rank_node_id AS taxon_rank_node_id, taxon.rank_uid AS taxon_rank_uid, taxon.rank_label AS taxon_rank_label, phenotype.entity_node_id, phenotype.entity_uid, phenotype.entity_label, phenotype.quality_node_id, phenotype.quality_uid, phenotype.quality_label, phenotype.related_entity_node_id, phenotype.related_entity_uid, phenotype.related_entity_label FROM annotation_source ";
            }
            
        } else {
            baseQuery = "(" + StringUtils.join(intersects, " INTERSECT ") + ") ";
        }
        final String query;
        if (this.totalOnly) {
            query = "SELECT count(*) FROM (" + baseQuery + ") AS query";
        } else { //TODO get sort column from config
            query = baseQuery + "JOIN asserted_taxon_annotation ON (asserted_taxon_annotation.annotation_id = annotation_source.annotation_id) " +
            "JOIN taxon ON (taxon.node_id = asserted_taxon_annotation.taxon_node_id) " +
            "JOIN phenotype ON (phenotype.node_id = asserted_taxon_annotation.phenotype_node_id) " +
            "ORDER BY " + "taxon_label" + " " + this.getSortText() + "LIMIT ? OFFSET ? " ;
        }
        return query;
    }

    private String getSortText() {
        return this.config.sortDescending() ? "DESC " : ""; 
    }

    private String getTaxaQuery(List<String> taxonIDs) {
        final StringBuffer query = new StringBuffer();
        query.append("SELECT * FROM annotation_source ");
        query.append("WHERE annotation_source.annotation_id IN ");
        query.append("(");
        query.append("SELECT asserted_taxon_annotation.annotation_id FROM asserted_taxon_annotation ");
        query.append("JOIN taxon ON (taxon.node_id = asserted_taxon_annotation.taxon_node_id) ");
        query.append("WHERE taxon.node_id IN ");
        query.append("(");
        query.append(String.format("SELECT taxon.node_id FROM taxon JOIN link taxon_is_a ON (taxon_is_a.node_id = taxon.node_id AND taxon_is_a.predicate_id = %s AND taxon_is_a.object_id IN  ", this.node(OBO.IS_A)));
        query.append("(");
        query.append(String.format("SELECT taxon.node_id FROM taxon WHERE uid IN %s ", this.createPlaceholdersList(this.config.getTaxonIDs().size())));
        query.append(")");
        query.append(")");
        query.append(")");
        query.append(")");
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
        query.append("(SELECT * FROM annotation_source ");
        query.append("WHERE annotation_source.annotation_id IN ");
        query.append("(");
        query.append("SELECT asserted_taxon_annotation.annotation_id FROM asserted_taxon_annotation ");
        if (phenotype.getEntityID() != null) {
            if (phenotype.includeEntityParts()) {
                query.append(String.format("JOIN link phenotype_inheres_in_part_of ON (phenotype_inheres_in_part_of.node_id = asserted_taxon_annotation.phenotype_node_id AND phenotype_inheres_in_part_of.predicate_id = %s) ", this.node(OBO.INHERES_IN_PART_OF)));    
            } else {
                query.append(String.format("JOIN link phenotype_inheres_in ON (phenotype_inheres_in.node_id = asserted_taxon_annotation.phenotype_node_id AND phenotype_inheres_in.predicate_id = %s) ", this.node(OBO.INHERES_IN)));
            }
        }
        if (phenotype.getQualityID() != null) {
            query.append(String.format("JOIN link quality_is_a ON (quality_is_a.node_id = asserted_taxon_annotation.phenotype_node_id AND quality_is_a.predicate_id = %s) ", this.node(OBO.IS_A)));
        }
        if (phenotype.getRelatedEntityID() != null) {
            query.append(String.format("JOIN link related_entity_towards ON (related_entity_towards.node_id = asserted_taxon_annotation.phenotype_node_id AND related_entity_towards.predicate_id = %s) ", this.node(OBO.TOWARDS)));  
        }
        query.append("WHERE ");
        query.append(this.translate(phenotype));
        query.append(") ");
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
            terms.add("related_entity_towards.object_id = " + NODE + " ");
        }
        buffer.append(StringUtils.join(terms, " AND "));
        buffer.append(")");
        return buffer.toString();
    }

}
