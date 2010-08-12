package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class IntersectionLinksQueryBuilder extends QueryBuilder {

    private final String uid;

    public IntersectionLinksQueryBuilder(String uid) {
        this.uid = uid;
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        statement.setString(1, this.uid);
    }

    @Override
    protected String getQuery() {
        return String.format(
                "SELECT relation.node_id AS relation_node_id, relation.uid AS relation_uid, relation.label AS relation_label, target.node_id AS other_node_id, target.uid AS other_uid, target.label AS other_label " +
                "FROM link " +
                "JOIN node relation ON (relation.node_id = link.predicate_id) " +
                "JOIN node target ON (target.node_id = link.object_id) " +
                "WHERE link.node_id = %s AND combinator = 'I'", NODE);
    }

}
