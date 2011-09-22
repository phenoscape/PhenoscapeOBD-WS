package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.phenoscape.obd.model.Vocab;

public class EntityFacetChildrenUIDsQueryBuilder extends QueryBuilder {

    final String termUID;
    final String relationUID;

    public EntityFacetChildrenUIDsQueryBuilder(String termUID, String relationUID) {
        this.termUID = termUID;
        this.relationUID = relationUID;
    }

    @Override
    protected String getQuery() {
        final StringBuilder query = new StringBuilder();
        query.append("SELECT subject.uid AS child_uid ");
        query.append("FROM link ");
        query.append(String.format("JOIN node object ON (object.node_id = %s AND link.object_id = object.node_id AND link.source_id IN (SELECT node_id FROM node WHERE uid in %s)) ", NODE, this.createPlaceholdersList(Vocab.ALL_ENTITY_NAMESPACES.size())));
        query.append("JOIN node subject ON (subject.node_id = link.node_id) ");
        query.append(String.format("WHERE link.predicate_id = %s", NODE));
        return query.toString();
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        int index = 1;
        statement.setString(index++, this.termUID);
        for (String namespace : Vocab.ALL_ENTITY_NAMESPACES) {
            statement.setString(index++, namespace);
        }
        statement.setString(index++, this.relationUID);
    }

}
