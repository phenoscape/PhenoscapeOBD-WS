package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.phenoscape.obd.model.Vocab.OBO;

public class GeneFacetChildrenUIDsQueryBuilder extends QueryBuilder {
    
    final String termUID;

    public GeneFacetChildrenUIDsQueryBuilder(String termUID) {
        this.termUID = termUID;
    }

    @Override
    protected String getQuery() {
        final StringBuilder query = new StringBuilder();
        query.append("SELECT subject.uid AS child_uid ");
        query.append("FROM link ");
        query.append("JOIN node subject ON (subject.node_id = link.node_id) ");
        query.append(String.format("WHERE link.predicate_id IN (SELECT node_id FROM node WHERE uid IN ('%s', '%s', '%s', '%s')) AND link.object_id = %s AND link.is_inferred = false", OBO.IS_A, OBO.HAS_FUNCTION, OBO.LOCATED_IN, OBO.PARTICIPATES_IN, NODE));
        return query.toString();
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        statement.setString(1, this.termUID);
    }

}
