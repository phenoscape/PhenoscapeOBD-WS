package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BulkTermNameQueryBuilder extends QueryBuilder {
    
    final String uid;
    
    public BulkTermNameQueryBuilder(String uid) {
        this.uid = uid;
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        statement.setString(1, this.uid);
    }

    @Override
    protected String getQuery() {
        return "SELECT term.*, source.uid AS source_uid, source.label AS source_label, taxon.uid AS taxon_uid, taxon.rank_node_id AS rank_node_id, taxon.rank_uid AS rank_uid, taxon.rank_label AS rank_label, taxon.is_extinct AS is_extinct " +
        " FROM node term " +
        " LEFT JOIN node source ON (term.source_id = source.node_id) " + 
        " LEFT JOIN taxon ON (taxon.node_id = term.node_id) " +
        " WHERE term.uid = ?"; 
    }

}
