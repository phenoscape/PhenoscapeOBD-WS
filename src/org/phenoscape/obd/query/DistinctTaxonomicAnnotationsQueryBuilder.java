package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.phenoscape.obd.model.PhenotypeSpec;
import org.phenoscape.obd.model.Vocab.OBO;
import org.phenoscape.obd.query.AnnotationsQueryConfig.SORT_COLUMN;

public class DistinctTaxonomicAnnotationsQueryBuilder extends QueryBuilder {

    private final AnnotationsQueryConfig config;
    private final boolean totalOnly;
    private final String annotationTable;
    private static final String JOIN = " JOIN taxon ON (taxon.node_id = taxon_node_id) JOIN phenotype ON (phenotype.node_id = phenotype_node_id) ";
    private static final Map<SORT_COLUMN, String> COLUMNS = new HashMap<SORT_COLUMN, String>();
    static {
        COLUMNS.put(SORT_COLUMN.TAXON, "taxon_label");
        COLUMNS.put(SORT_COLUMN.ENTITY, "entity_label");
        COLUMNS.put(SORT_COLUMN.QUALITY, "quality_label");
        COLUMNS.put(SORT_COLUMN.RELATED_ENTITY, "related_entity_label");
    }

    public DistinctTaxonomicAnnotationsQueryBuilder(AnnotationsQueryConfig config, boolean totalOnly) {
        this.config = config;
        this.totalOnly = totalOnly;
        this.annotationTable = config.includeInferredAnnotations() ? "taxon_annotation" : "asserted_taxon_annotation";
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
            baseQuery = String.format("SELECT taxon.node_id AS taxon_node_id, taxon.uid AS taxon_uid, taxon.label AS taxon_label, taxon.is_extinct AS taxon_is_extinct, taxon.rank_node_id AS taxon_rank_node_id, taxon.rank_uid AS taxon_rank_uid, taxon.rank_label AS taxon_rank_label, phenotype.entity_node_id, phenotype.entity_uid, phenotype.entity_label, phenotype.quality_node_id, phenotype.quality_uid, phenotype.quality_label, phenotype.related_entity_node_id, phenotype.related_entity_uid, phenotype.related_entity_label FROM %s", this.annotationTable);
        } else {
            baseQuery = "SELECT taxon.node_id AS taxon_node_id, taxon.uid AS taxon_uid, taxon.label AS taxon_label, taxon.is_extinct AS taxon_is_extinct, taxon.rank_node_id AS taxon_rank_node_id, taxon.rank_uid AS taxon_rank_uid, taxon.rank_label AS taxon_rank_label, phenotype.entity_node_id, phenotype.entity_uid, phenotype.entity_label, phenotype.quality_node_id, phenotype.quality_uid, phenotype.quality_label, phenotype.related_entity_node_id, phenotype.related_entity_uid, phenotype.related_entity_label FROM (" + StringUtils.join(intersects, " INTERSECT ") + ") AS query ";
        }
        final String query;
        if (this.totalOnly) {
            if (intersects.isEmpty()) {
                query = String.format("SELECT count(*) from %s", this.annotationTable);
            } else {
                query = "SELECT count(*) FROM (" + StringUtils.join(intersects, " INTERSECT ") + ") AS query";
            }
        } else {
            query = baseQuery + JOIN + "ORDER BY " + COLUMNS.get(this.config.getSortColumn()) + " " + this.getSortText() + "LIMIT ? OFFSET ? " ;
        }
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
        query.append(String.format("SELECT %s.* from %s ", this.annotationTable, this.annotationTable));
        query.append(String.format(" WHERE %s.taxon_node_id IN ", this.annotationTable));
        query.append("(");
        query.append(String.format("SELECT taxon_is_a.node_id FROM link taxon_is_a WHERE (taxon_is_a.predicate_id = %s AND taxon_is_a.object_id = %s) ", this.node(OBO.IS_A), NODE));
        query.append(") ");
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
        query.append(String.format("SELECT %s.* FROM %s ", this.annotationTable, this.annotationTable));
        query.append(String.format(" WHERE %s.phenotype_node_id IN ", this.annotationTable));
        query.append("(");
        query.append("SELECT phenotype.node_id from phenotype ");
        if (phenotype.getEntityID() != null) {
            if (phenotype.includeEntityParts()) {
                query.append(String.format("JOIN link phenotype_inheres_in_part_of ON (phenotype_inheres_in_part_of.node_id = phenotype.node_id AND phenotype_inheres_in_part_of.predicate_id = %s) ", this.node(OBO.INHERES_IN_PART_OF)));    
            } else {
                query.append(String.format("JOIN link phenotype_inheres_in ON (phenotype_inheres_in.node_id = phenotype.node_id AND phenotype_inheres_in.predicate_id = %s) ", this.node(OBO.INHERES_IN)));
            }
        }
        if (phenotype.getQualityID() != null) {
            query.append(String.format("JOIN link quality_is_a ON (quality_is_a.node_id = phenotype.node_id AND quality_is_a.predicate_id = %s) ", this.node(OBO.IS_A)));
        }
        if (phenotype.getRelatedEntityID() != null) {
            query.append(String.format("JOIN link related_entity_towards ON (related_entity_towards.node_id = phenotype.node_id AND related_entity_towards.predicate_id = %s) ", this.node(OBO.TOWARDS)));  
        }
        query.append(" WHERE ");
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

    private String getPublicationsQuery(List<String> publicationIDs) {
        final StringBuffer query = new StringBuffer();
        query.append("(");
        final List<String> subQueries = new ArrayList<String>();
        for (String publicationID : publicationIDs) {
            subQueries.add(this.getPublicationQuery(publicationID));
        }
        query.append(StringUtils.join(subQueries, " UNION "));
        query.append(")");
        return query.toString();
    }

    private String getPublicationQuery(String publicationID) {
        final StringBuffer query = new StringBuffer();
        query.append("(");
        query.append(String.format("SELECT %s.* FROM %s ", this.annotationTable, this.annotationTable));
        query.append(String.format(" WHERE %s.annotation_id IN ", this.annotationTable));
        query.append("(");
        query.append("SELECT annotation_source.annotation_id FROM annotation_source ");
        query.append(String.format(" WHERE annotation_source.publication_node_id = %s ", NODE));
        query.append(")");
        query.append(") ");
        return query.toString();
    }

}
