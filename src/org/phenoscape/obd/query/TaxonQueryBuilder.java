package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class TaxonQueryBuilder extends QueryBuilder {

    final String uid;

    public TaxonQueryBuilder(String uid) {
        this.uid = uid;
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        statement.setString(1, uid);
    }

    @Override
    protected String getQuery() {
        return "SELECT focal_taxon.*, parent.uid AS parent_uid, parent.label AS parent_label, parent.is_extinct AS parent_is_extinct, parent.rank_node_id AS parent_rank_node_id, parent.rank_uid AS parent_rank_uid, parent.rank_label AS parent_rank_label, source.uid AS source_uid " +
        "FROM taxon focal_taxon " +
        " JOIN node taxon_node ON (taxon_node.node_id = focal_taxon.node_id) " +
        "LEFT OUTER JOIN taxon parent ON (parent.node_id = focal_taxon.parent_node_id) " +
        "LEFT OUTER JOIN node source ON (source.node_id = taxon_node.source_id)" +
        "WHERE focal_taxon.uid = ?";
    }

}
