package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.phenoscape.obd.model.PhenotypeSpec;
import org.phenoscape.obd.model.Vocab.OBO;

public class DistinctGenePhenotypesQueryBuilder extends QueryBuilder {

    private final AnnotationsQueryConfig config;
    private final boolean totalOnly;

    public DistinctGenePhenotypesQueryBuilder(AnnotationsQueryConfig config, boolean totalOnly) {
        this.config = config;
        this.totalOnly = totalOnly;
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        int index = 1;
        for (String geneID : this.config.getGeneIDs()) {
            statement.setString(index++, geneID);
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
        if (!this.config.getGeneIDs().isEmpty()) {
            intersects.add(this.getGenesQuery(this.config.getGeneIDs()));
        }
        if (!this.config.getPhenotypes().isEmpty()) {
            intersects.add(this.getPhenotypesQuery(this.config.getPhenotypes()));
        }
        final String baseQuery;
        if (intersects.isEmpty()) {
            baseQuery = "SELECT DISTINCT distinct_gene_annotation.phenotype_node_id, distinct_gene_annotation.phenotype_uid, distinct_gene_annotation.phenotype_label FROM distinct_gene_annotation ";    
        } else {
            baseQuery = "(" + StringUtils.join(intersects, " INTERSECT ") + ") ";
        }
        final String query;
        if (this.totalOnly) {
            query = "SELECT count(*) FROM (" + baseQuery + ") AS query";
        } else {
            query = baseQuery + "ORDER BY " + "phenotype_uid" + " " + this.getSortText() + "LIMIT ? OFFSET ? " ;
        }
        return query;
    }

    private String getSortText() {
        return this.config.sortDescending() ? "DESC " : ""; 
    }

    private String getGenesQuery(List<String> genes) {
      //FIXME doesn't properly handle ANY/ALL ?
        final StringBuffer query = new StringBuffer();
        query.append("(SELECT DISTINCT distinct_gene_annotation.phenotype_node_id, distinct_gene_annotation.phenotype_uid, distinct_gene_annotation.phenotype_label FROM distinct_gene_annotation WHERE ");
        query.append("distinct_gene_annotation.gene_uid IN ");
        query.append(this.createPlaceholdersList(this.config.getGeneIDs().size()));
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
        query.append("(SELECT DISTINCT distinct_gene_annotation.phenotype_node_id, distinct_gene_annotation.phenotype_uid, distinct_gene_annotation.phenotype_label FROM distinct_gene_annotation ");
        if (phenotype.getEntityID() != null) {
            if (phenotype.includeEntityParts()) {
                query.append(String.format("JOIN link phenotype_inheres_in_part_of ON (phenotype_inheres_in_part_of.node_id = distinct_gene_annotation.phenotype_node_id AND phenotype_inheres_in_part_of.predicate_id = %s) ", this.node(OBO.INHERES_IN_PART_OF)));    
            } else {
                query.append(String.format("JOIN link phenotype_inheres_in ON (phenotype_inheres_in.node_id = distinct_gene_annotation.phenotype_node_id AND phenotype_inheres_in.predicate_id = %s) ", this.node(OBO.INHERES_IN)));
            }
        }
        if (phenotype.getQualityID() != null) {
            query.append(String.format("JOIN link quality_is_a ON (quality_is_a.node_id = distinct_gene_annotation.quality_node_id AND quality_is_a.predicate_id = %s) ", this.node(OBO.IS_A)));
        }
        if (phenotype.getRelatedEntityID() != null) {
            query.append(String.format("JOIN link related_entity_is_a ON (related_entity_is_a.node_id = distinct_gene_annotation.related_entity_node_id AND related_entity_is_a.predicate_id = %s) ", this.node(OBO.IS_A)));  
        }
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
            terms.add("related_entity_is_a.object_id = " + NODE + " ");
        }
        buffer.append(StringUtils.join(terms, " AND "));
        buffer.append(")");
        return buffer.toString();
    }

    private Logger log() {
        return Logger.getLogger(this.getClass());
    }

}
