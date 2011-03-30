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

public class AnnotatedTaxaQueryBuilder extends QueryBuilder {

    private final AnnotationsQueryConfig config;
    private final boolean totalOnly;
    private final String annotationTable;
    private static final Map<SORT_COLUMN, String> COLUMNS = new HashMap<SORT_COLUMN, String>();
    static {
        COLUMNS.put(SORT_COLUMN.TAXON, "label");
        COLUMNS.put(SORT_COLUMN.FAMILY, "family_label");
        COLUMNS.put(SORT_COLUMN.ORDER, "order_label");
    }

    public AnnotatedTaxaQueryBuilder(AnnotationsQueryConfig config, boolean totalOnly) {
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
        if (!this.totalOnly && this.hasLimit()) {
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
            baseQuery = String.format("SELECT * FROM taxon WHERE taxon.node_id in (SELECT taxon_node_id FROM %s) ", this.annotationTable);    
        } else {
            baseQuery = "(" + StringUtils.join(intersects, " INTERSECT ") + ") ";
        }
        final String query;
        if (this.totalOnly) {
            if (intersects.isEmpty()) {
                query = String.format("SELECT count(*) FROM taxon WHERE taxon.node_id in (SELECT taxon_node_id FROM %s) ", this.annotationTable);    
            } else {
                query = "SELECT count(*) FROM " + "(" + baseQuery + ")" + "AS query";
            }

        } else {
            query = "(" + baseQuery + ") " + "ORDER BY " + COLUMNS.get(this.config.getSortColumn()) + " " + this.getSortText() + this.getLimitText();
        }
        return query;
    }
    
    private boolean hasLimit() {
        return this.config.getLimit() > -1;
    }
    
    private String getLimitText() {
        if (this.hasLimit()) {
            return "LIMIT ? OFFSET ? ";
        } else {
            return "";
        }
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

    private String getTaxonQuery(String taxonID) { //FIXME this should be limited to taxa with phenotypes
        final StringBuffer query = new StringBuffer();
        query.append("(");
        query.append("SELECT taxon.* FROM taxon ");
        query.append("WHERE taxon.node_id IN ");
        query.append("(");
        query.append(String.format("SELECT taxon_is_a.node_id FROM link taxon_is_a WHERE (taxon_is_a.predicate_id = %s AND taxon_is_a.object_id = %s) ", this.node(OBO.IS_A), NODE));
        query.append(")");
        query.append(") ");
        return query.toString();
    }

    private String getPhenotypesQuery(List<PhenotypeSpec> phenotypes) {
        final StringBuffer query = new StringBuffer();
        query.append("(");
        final List<String> subQueries = new ArrayList<String>();
        for (PhenotypeSpec phenotype : phenotypes) {
            subQueries.add(this.getPhenotypeQuery(phenotype));
        }
        query.append(StringUtils.join(subQueries, (this.config.matchAllPhenotypes() ? " INTERSECT " : " UNION ")));
        query.append(")");
        return query.toString();
    }

    private String getPhenotypeQuery(PhenotypeSpec phenotype) {
        final StringBuffer query = new StringBuffer();
        query.append("(");
        query.append("SELECT taxon.* FROM taxon ");
        query.append("WHERE taxon.node_id IN ");
        query.append("(");
        query.append(String.format("SELECT taxon_node_id FROM %s ", this.annotationTable));
        if (phenotype.getEntityID() != null) {
            if (phenotype.includeEntityParts()) {
                query.append(String.format("JOIN link phenotype_inheres_in_part_of ON (phenotype_inheres_in_part_of.node_id = %s.phenotype_node_id AND phenotype_inheres_in_part_of.predicate_id = %s) ", this.annotationTable, this.node(OBO.INHERES_IN_PART_OF)));    
            } else {
                query.append(String.format("JOIN link phenotype_inheres_in ON (phenotype_inheres_in.node_id = %s.phenotype_node_id AND phenotype_inheres_in.predicate_id = %s) ", this.annotationTable, this.node(OBO.INHERES_IN)));
            }
        }
        if (phenotype.getQualityID() != null) {
            query.append(String.format("JOIN link quality_is_a ON (quality_is_a.node_id = %s.phenotype_node_id AND quality_is_a.predicate_id = %s) ", this.annotationTable, this.node(OBO.IS_A)));
        }
        if (phenotype.getRelatedEntityID() != null) {
            query.append(String.format("JOIN link related_entity_towards ON (related_entity_towards.node_id = %s.phenotype_node_id AND related_entity_towards.predicate_id = %s) ", this.annotationTable, this.node(OBO.TOWARDS)));  
        }
        query.append("WHERE ");
        query.append(this.translate(phenotype));
        query.append(")");
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
        query.append(StringUtils.join(subQueries, (this.config.matchAllPublications() ? " INTERSECT " : " UNION ")));
        query.append(")");
        return query.toString();
    }

    private String getPublicationQuery(String publicationID) {
        final StringBuffer query = new StringBuffer();
        query.append("(");
        query.append("SELECT taxon.* from taxon ");
        query.append("WHERE taxon.node_id IN ");
        query.append("(");
        query.append(String.format("SELECT taxon_node_id FROM %s ", this.annotationTable));
        query.append(String.format(" JOIN annotation_source ON (annotation_source.annotation_id = %s.annotation_id and annotation_source.publication_node_id = %s) ", this.annotationTable, NODE));
        query.append(")");
        query.append(")");
        return query.toString();
    }

}
