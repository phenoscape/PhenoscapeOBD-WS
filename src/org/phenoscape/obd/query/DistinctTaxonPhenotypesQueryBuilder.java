package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.phenoscape.obd.model.PhenotypeSpec;
import org.phenoscape.obd.model.Vocab.OBO;

public class DistinctTaxonPhenotypesQueryBuilder extends QueryBuilder {

    private final AnnotationsQueryConfig config;
    private final boolean totalOnly;

    public DistinctTaxonPhenotypesQueryBuilder(AnnotationsQueryConfig config, boolean totalOnly) {
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
            baseQuery = "SELECT * FROM phenotype WHERE phenotype.node_id IN (SELECT phenotype_node_id FROM asserted_taxon_annotation) ";
        } else {
            baseQuery = "(" + StringUtils.join(intersects, " INTERSECT ") + ") ";
        }
        final String query;
        if (this.totalOnly) {
            query = "SELECT count(*) FROM (" + baseQuery + ") AS query";
        } else {
            query = baseQuery + "ORDER BY " + "uid" + " " + this.getSortText() + "LIMIT ? OFFSET ? " ;
        }
        return query;
    }

    private String getSortText() {
        return this.config.sortDescending() ? "DESC " : ""; 
    }

    private String getTaxaQuery(List<String> taxonIDs) {
        final StringBuffer query = new StringBuffer();
        query.append("SELECT * FROM phenotype ");
        query.append("WHERE phenotype.node_id IN ");
        query.append("(");
        query.append("SELECT phenotype_node_id FROM asserted_taxon_annotation ");
        query.append("WHERE asserted_taxon_annotation.taxon_node_id IN ");
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
        query.append("SELECT * FROM phenotype ");
        query.append("WHERE phenotype.node_id IN ");
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
        query.append("(SELECT phenotype.node_id FROM phenotype ");
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
        query.append("JOIN asserted_taxon_annotation ON (asserted_taxon_annotation.phenotype_node_id = phenotype.node_id) ");
        query.append("WHERE ");
        query.append(this.translate(phenotype));
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
        query.append(" SELECT * FROM phenotype ");
        query.append(" WHERE phenotype.node_id IN ");
        query.append("(");
        query.append("SELECT phenotype_node_id FROM asserted_taxon_annotation ");
        query.append("JOIN annotation_source ON (annotation_source.annotation_id = asserted_taxon_annotation.annotation_id ) ");
        query.append(String.format(" WHERE annotation_source.publication_node_id = %s ", NODE));
        query.append(")");
        query.append(") ");
        return query.toString();
    }

}
