package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.phenoscape.obd.query.GeneAnnotationsQueryConfig.SORT_COLUMN;

public class GeneAnnotationsQueryBuilder extends QueryBuilder {

    private final GeneAnnotationsQueryConfig config;
    private final boolean totalOnly;
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
        //TODO
        int index = 1;
        for (String geneID : this.config.getGeneIDs()) {
            statement.setString(index++, geneID);
        }
        if (!this.totalOnly) {
            statement.setInt(index++, this.config.getLimit());
            statement.setInt(index++, this.config.getIndex());
        }
    }

    @Override
    protected String getQuery() {
        //TODO
        final String baseQuery = "SELECT DISTINCT distinct_gene_annotation.* " + 
        "FROM distinct_gene_annotation " +
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

    private String createWhereClause() {
        final StringBuffer where = new StringBuffer();
        if (!this.config.getGeneIDs().isEmpty()) {
            where.append("WHERE ");
            where.append("distinct_gene_annotation.gene_uid IN ");
            where.append(this.createPlaceholders(this.config.getGeneIDs().size()));
        }
        return where.toString();
    }
    
    private String createPlaceholders(int count) {
        final StringBuffer buffer = new StringBuffer();
        buffer.append("(");
        for (int i = 0; i < count; i++) {
            buffer.append("?");
            if ((i + 1) < count) { buffer.append(", "); }
        }
        buffer.append(") ");
        return buffer.toString();
    }
    
    private Logger log() {
        return Logger.getLogger(this.getClass());
    }

}
