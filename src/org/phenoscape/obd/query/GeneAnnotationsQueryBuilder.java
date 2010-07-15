package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.phenoscape.obd.model.PhenotypeSpec;
import org.phenoscape.obd.model.Vocab.OBO;
import org.phenoscape.obd.query.GeneAnnotationsQueryConfig.SORT_COLUMN;

public class GeneAnnotationsQueryBuilder extends QueryBuilder {

    private final GeneAnnotationsQueryConfig config;
    private final boolean totalOnly;
    private static final String NODE = "(SELECT node.node_id FROM node WHERE node.uid=?)";
    private static final Map<SORT_COLUMN, String> COLUMNS = new HashMap<SORT_COLUMN, String>();
    static {
        COLUMNS.put(SORT_COLUMN.GENE, "distinct_gene_annotation.gene_label");
        COLUMNS.put(SORT_COLUMN.ENTITY, "distinct_gene_annotation.entity_label");
        COLUMNS.put(SORT_COLUMN.QUALITY, "distinct_gene_annotation.quality_label");
        COLUMNS.put(SORT_COLUMN.RELATED_ENTITY, "distinct_gene_annotation.related_entity_label");
    }

    public GeneAnnotationsQueryBuilder(GeneAnnotationsQueryConfig config, boolean totalOnly) {
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
        final String baseQuery = "SELECT DISTINCT distinct_gene_annotation.* " + 
        "FROM distinct_gene_annotation " +
        this.createJoins() +
        this.createWhereClause();
        final String query;
        if (this.totalOnly) {
            query = "SELECT count(*) FROM (" + baseQuery + ") AS query";
        } else {
            query = baseQuery + "ORDER BY " + COLUMNS.get(this.config.getSortColumn()) + " " + "LIMIT ? OFFSET ?";
        }
        log().debug("Query: " + query);
        return query;
    }

    private String createJoins() {
        final StringBuffer joins = new StringBuffer();
        if (this.needsIsA()) {
            joins.append(String.format("JOIN node is_a ON (is_a.uid = '%s') ", OBO.IS_A));
        }
        if (this.needsInheresIn()) {
            joins.append(String.format("JOIN node inheres_in ON (inheres_in.uid = '%s') ", OBO.INHERES_IN));
            joins.append("JOIN link phenotype_inheres_in ON (phenotype_inheres_in.node_id = distinct_gene_annotation.phenotype_node_id AND phenotype_inheres_in.predicate_id = inheres_in.node_id) ");
        }
        if (this.needsInheresInPartOf()) {
            joins.append(String.format("JOIN node inheres_in_part_of ON (inheres_in_part_of.uid = '%s') ", OBO.INHERES_IN_PART_OF));
            joins.append("JOIN link phenotype_inheres_in_part_of ON (phenotype_inheres_in_part_of.node_id = distinct_gene_annotation.phenotype_node_id AND phenotype_inheres_in_part_of.predicate_id = inheres_in_part_of.node_id) ");
        }
        if (this.hasQualities(this.config)) {
            joins.append("JOIN link quality_is_a ON (quality_is_a.node_id = distinct_gene_annotation.quality_node_id AND quality_is_a.predicate_id = is_a.node_id) ");
        }
        if (this.hasRelatedEntities(this.config)) {
            joins.append("JOIN link related_entity_is_a ON (related_entity_is_a.node_id = distinct_gene_annotation.related_entity_node_id AND related_entity_is_a.predicate_id = is_a.node_id) ");
        }
        return joins.toString();
    }

    private String createWhereClause() {
        final StringBuffer where = new StringBuffer();
        where.append("WHERE ");
        if (!this.config.getGeneIDs().isEmpty()) {
            where.append("distinct_gene_annotation.gene_uid IN ");
            where.append(this.createPlaceholdersList(this.config.getGeneIDs().size()));
        }
        if (!config.getPhenotypes().isEmpty()) {
            if (!this.config.getGeneIDs().isEmpty()) {
                where.append("AND ");
            }
            where.append("(");
            final Iterator<PhenotypeSpec> phenotypes = this.config.getPhenotypes().iterator();
            while (phenotypes.hasNext()) {
                where.append(this.translate(phenotypes.next()));
                if (phenotypes.hasNext()) {
                    where.append(" OR ");
                }
            }
            where.append(") ");
        }
        return where.toString();
    }

    private boolean needsIsA() {
        return this.hasQualities(config) || this.hasRelatedEntities(config);
    }

    private boolean needsInheresIn() {
        return this.hasIsAOnlyEntities(config);
    }

    private boolean needsInheresInPartOf() {
        return this.hasPartOfEntities(config);
    }

    private boolean hasIsAOnlyEntities(GeneAnnotationsQueryConfig config) {
        for (PhenotypeSpec phenotype : config.getPhenotypes()) {
            if ((phenotype.getEntityID() != null) && (!phenotype.includeEntityParts())) {
                return true;
            }
        }
        return false;
    }

    private boolean hasPartOfEntities(GeneAnnotationsQueryConfig config) {
        for (PhenotypeSpec phenotype : config.getPhenotypes()) {
            if ((phenotype.getEntityID() != null) && (phenotype.includeEntityParts())) {
                return true;
            }
        }
        return false;
    }

    private boolean hasQualities(GeneAnnotationsQueryConfig config) {
        for (PhenotypeSpec phenotype : config.getPhenotypes()) {
            if (phenotype.getQualityID() != null) return true;
        }
        return false;
    }

    private boolean hasRelatedEntities(GeneAnnotationsQueryConfig config) {
        for (PhenotypeSpec phenotype : config.getPhenotypes()) {
            if (phenotype.getRelatedEntityID() != null) return true;
        }
        return false;
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
