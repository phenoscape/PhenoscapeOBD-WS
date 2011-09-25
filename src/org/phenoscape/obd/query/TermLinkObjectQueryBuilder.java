package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;

import org.phenoscape.obd.model.DefaultTerm;

public class TermLinkObjectQueryBuilder extends QueryBuilder {

    final DefaultTerm term;
    final Collection<String> sourceNamespaces;

    public TermLinkObjectQueryBuilder(DefaultTerm term, Collection<String> namespaces) {
        this.term = term;
        this.sourceNamespaces = namespaces;
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        int index = 0;
        statement.setInt(++index, this.term.getNodeID());
        for (String namespace : this.sourceNamespaces) {
            statement.setString(++index, namespace);
        }
    }

    @Override
    protected String getQuery() {
        final StringBuffer query = new StringBuffer();
        query.append("SELECT relation.node_id AS relation_node_id, relation.uid AS relation_uid, relation.label AS relation_label, subject.node_id AS other_node_id, subject.uid AS other_uid, subject.label AS other_label ");
        query.append("FROM link ");
        query.append("JOIN node relation ON (relation.node_id = link.predicate_id) ");
        query.append("JOIN node subject ON (subject.node_id = link.node_id) ");
        query.append(String.format("WHERE link.combinator = '' AND link.object_id = ? AND link.source_id IN (SELECT node_id FROM node WHERE uid IN %s)", this.createPlaceholdersList(this.sourceNamespaces.size())));
        return query.toString();
    }

}
