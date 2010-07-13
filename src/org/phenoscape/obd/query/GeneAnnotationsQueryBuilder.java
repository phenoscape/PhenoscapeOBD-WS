package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.phenoscape.obd.query.GeneAnnotationsQueryConfig.SORT_COLUMN;

public class GeneAnnotationsQueryBuilder extends QueryBuilder {

    private final GeneAnnotationsQueryConfig config;
    private static final Map<SORT_COLUMN, String> COLUMNS = new HashMap<SORT_COLUMN, String>();
    static {
        COLUMNS.put(SORT_COLUMN.GENE, "distinct_gene_annotation.gene_label");
        COLUMNS.put(SORT_COLUMN.ENTITY, "distinct_gene_annotation.entity_label");
        COLUMNS.put(SORT_COLUMN.QUALITY, "distinct_gene_annotation.quality_label");
        COLUMNS.put(SORT_COLUMN.RELATED_ENTITY, "distinct_gene_annotation.related_entity_label");
    }

    public GeneAnnotationsQueryBuilder(GeneAnnotationsQueryConfig config) {
        this.config = config;
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        //TODO
        // maybe first shouldn't be a string parameter?
        statement.setString(1, COLUMNS.get(this.config.getSortColumn()));
        statement.setInt(2, this.config.getLimit());
        statement.setInt(3, this.config.getIndex());
    }

    @Override
    protected String getQuery() {
        //TODO
        return "SELECT distinct_gene_annotation.* FROM distinct_gene_annotation " +
        "ORDER BY ? " +
        "LIMIT ? OFFSET ?";
    }

}
