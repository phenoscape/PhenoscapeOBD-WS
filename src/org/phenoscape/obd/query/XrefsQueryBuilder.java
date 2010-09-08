package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.phenoscape.obd.model.DefaultTerm;
import org.phenoscape.obd.model.Vocab.OBO;

public class XrefsQueryBuilder extends QueryBuilder {

    final DefaultTerm term;

    public XrefsQueryBuilder(DefaultTerm term) {
        this.term = term;
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        statement.setInt(1, this.term.getNodeID());
    }

    @Override
    protected String getQuery() {
        return  String.format("SELECT link.*, xref.uid AS xref_uid FROM link LEFT JOIN node xref ON (link.object_id = xref.node_id AND link.predicate_id = %s) WHERE link.node_id = ? AND link.is_inferred = false AND xref.node_id IS NOT NULL", this.node(OBO.HAS_DBXREF));
    }

}
