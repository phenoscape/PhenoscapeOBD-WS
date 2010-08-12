package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.phenoscape.obd.model.DefaultTerm;

public class TermLinkSubjectQueryBuilder extends QueryBuilder {

    final DefaultTerm term;

    public TermLinkSubjectQueryBuilder(DefaultTerm term) {
        this.term = term;
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        statement.setInt(1, this.term.getNodeID());
        statement.setInt(2, this.term.getSourceID());
    }

    @Override
    protected String getQuery() {
        return "SELECT relation.node_id AS relation_node_id, relation.uid AS relation_uid, relation.label AS relation_label, target.node_id AS other_node_id, target.uid AS other_uid, target.label AS other_label " +
        "FROM link " +
        "JOIN node relation ON (relation.node_id = link.predicate_id) " +
        "JOIN node target ON (target.node_id = link.object_id) " +
        "WHERE link.node_id = ? AND link.source_id = ?";
    }

}
