package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.phenoscape.obd.model.DefaultTerm;

public class SynonymsQueryBuilder extends QueryBuilder {

    final DefaultTerm term;

    public SynonymsQueryBuilder(DefaultTerm term) {
        this.term = term;
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        statement.setInt(1, this.term.getNodeID());
    }

    @Override
    protected String getQuery() {
        return  "SELECT alias.*, type.uid AS type_uid FROM alias LEFT JOIN node type ON (alias.type_id = type.node_id) WHERE alias.node_id = ?";
    }

}
