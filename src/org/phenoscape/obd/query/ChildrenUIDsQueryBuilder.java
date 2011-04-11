package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ChildrenUIDsQueryBuilder extends QueryBuilder {

    final String termUID;
    final String relationUID;

    public ChildrenUIDsQueryBuilder(String termUID, String relationUID) {
        this.termUID = termUID;
        this.relationUID = relationUID;
    }

    @Override
    protected String getQuery() {
        final StringBuilder query = new StringBuilder();
        query.append("SELECT subject.uid AS child_uid ");
        query.append("FROM link ");
        query.append(String.format("JOIN node object ON (object.node_id = %s AND link.object_id = object.node_id AND link.source_id = object.source_id) ", NODE));
        query.append("JOIN node subject ON (subject.node_id = link.node_id) ");
        query.append(String.format("WHERE link.predicate_id = %s", NODE));
        return query.toString();
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        statement.setString(1, this.termUID);
        statement.setString(2, this.relationUID);
    }

}
