package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class AutocompleteNameQueryBuilder extends QueryBuilder {

    final SearchConfig config;
    final boolean startsWith;

    public AutocompleteNameQueryBuilder(SearchConfig config, boolean startsWith) {
        super();
        this.config = config;
        this.startsWith = startsWith;
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        final String searchText = this.startsWith ? (this.config.getSearchText().toLowerCase() + "%") : ("%" + this.config.getSearchText().toLowerCase() + "%");
        statement.setString(1, searchText);
        int index = 2;
        for (String namespace : this.config.getNamespaces()) {
            statement.setString(index, namespace);
            index++;
        }
    }

    @Override
    protected String getQuery() {
        final String query = 
            "SELECT term.*, source.uid AS source_uid, source.label AS source_label " +
            "FROM node term " +
            "JOIN node source ON (term.source_id = source.node_id) " +
            "WHERE lower(term.label) LIKE ? " + "AND source.uid IN " + this.createNamespacePlaceholders(config.getNamespaces().size()) + " AND term.is_obsolete = false";
        return query;
    }

    private String createNamespacePlaceholders(int count) {
        final StringBuffer buffer = new StringBuffer();
        buffer.append("(");
        for (int i = 0; i < count; i++) {
            buffer.append("?");
            if ((i + 1) < count) { buffer.append(", "); }
        }
        buffer.append(")");
        return buffer.toString();
    }

}
