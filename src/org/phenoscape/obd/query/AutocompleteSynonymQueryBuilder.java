package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class AutocompleteSynonymQueryBuilder extends QueryBuilder {

    final SearchConfig config;
    final boolean startsWith;

    public AutocompleteSynonymQueryBuilder(SearchConfig config, boolean startsWith) {
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
            "SELECT term.*, alias.label AS synonym_label, source.uid AS source_uid " +
            "FROM node term " +
            "JOIN node source ON (term.source_id = source.node_id) " +
            "JOIN alias ON (term.node_id = alias.node_id) " +
            "WHERE lower(alias.label) LIKE ? " + "AND source.uid IN " + this.createPlaceholdersList(config.getNamespaces().size());
        return query;
    }

}
