package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

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
        if (!this.totalOnly) {
            statement.setInt(1, this.config.getLimit());
            statement.setInt(2, this.config.getIndex());
        }
    }

    @Override
    protected String getQuery() {
        //TODO
        final String select = this.totalOnly ? "SELECT count(distinct_gene_annotation.*) " : "SELECT distinct_gene_annotation.* ";
        final String ending = this.totalOnly ? "" : "ORDER BY " + COLUMNS.get(this.config.getSortColumn()) + " " + "LIMIT ? OFFSET ?";
        return select + "FROM distinct_gene_annotation " + ending;

    }

}
