package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.phenoscape.obd.model.Vocab.DC;

public class PublicationTermQueryBuilder extends QueryBuilder {
    
    final String uid;

    public PublicationTermQueryBuilder(String uid) {
        this.uid = uid;
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        statement.setString(1, uid);
    }

    @Override
    protected String getQuery() {
        final StringBuffer query = new StringBuffer();
        query.append("SELECT publication.node_id, publication.uid, publication.source_id, publication.label, citation.val AS citation_label, abstract.val AS abstract_label, doi.val AS doi, source.uid AS source_uid, source.label AS source_label ");
        query.append("FROM node publication ");
        query.append(String.format("LEFT OUTER JOIN tagval citation ON (citation.tag_id = %s AND citation.node_id = publication.node_id) ", this.node(DC.CITATION)));
        query.append(String.format("LEFT OUTER JOIN tagval abstract ON (abstract.tag_id = %s AND abstract.node_id = publication.node_id) ", this.node(DC.ABSTRACT)));
        query.append(String.format("LEFT OUTER JOIN tagval doi ON (doi.tag_id = %s AND doi.node_id = publication.node_id) ", this.node(DC.IDENTIFIER)));
        query.append("LEFT OUTER JOIN node source ON (source.node_id = publication.source_id)" );
        query.append("WHERE publication.uid = ?");
        return query.toString();
    }

}
