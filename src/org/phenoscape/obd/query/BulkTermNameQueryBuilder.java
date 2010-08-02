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
        return "SELECT term.*, source.node_id AS source_node_id, source.uid AS source_uid, rank.node_id AS rank_node_id, rank.uid AS rank_uid, rank.label AS rank_label, CAST (tagval.val AS BOOLEAN) AS is_extinct " +
        " FROM node term " +
        " JOIN node source ON (source.node_id = term.source_id) " +
        " LEFT JOIN link has_rank_link ON (has_rank_link.node_id = term.node_id AND has_rank_link.predicate_id = (SELECT node.node_id FROM node WHERE node.uid='has_rank') AND has_rank_link.is_inferred = false) " +
        " LEFT JOIN node rank ON (rank.node_id = has_rank_link.object_id) " +
        " LEFT JOIN tagval ON (tagval.node_id = term.node_id AND tagval.tag_id = (SELECT node.node_id FROM node WHERE node.uid='is_extinct')) " +
        " WHERE term.uid = ?"; 
    }

}
