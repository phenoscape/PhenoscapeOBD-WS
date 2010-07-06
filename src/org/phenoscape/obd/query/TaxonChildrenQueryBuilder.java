package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.phenoscape.obd.model.TaxonTerm;

public class TaxonChildrenQueryBuilder extends QueryBuilder {

    final TaxonTerm taxon;

    public TaxonChildrenQueryBuilder(TaxonTerm taxon) {
        this.taxon = taxon;
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        statement.setInt(1, this.taxon.getNodeID());
    }

    @Override
    protected String getQuery() {
        return "SELECT * " +
        "FROM taxon " +
        "WHERE parent_node_id = ?";
    }

}
