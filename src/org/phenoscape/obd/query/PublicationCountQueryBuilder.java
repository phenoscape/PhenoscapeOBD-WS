package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PublicationCountQueryBuilder extends QueryBuilder {
    
    final String taxonID;

    public PublicationCountQueryBuilder(String taxonID) {
        this.taxonID = taxonID;
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        if (!(this.taxonID == null)) {
            statement.setString(1, this.taxonID);
        }
    }

    @Override
    protected String getQuery() {
        final StringBuffer query = new StringBuffer();
        query.append("SELECT count(DISTINCT publication_uid) FROM queryable_taxon_annotation ");
        if (!(this.taxonID == null)) {
            query.append("JOIN link taxon_is_a ON (taxon_is_a.node_id = queryable_taxon_annotation.taxon_node_id AND taxon_is_a.predicate_id = (SELECT node.node_id FROM node WHERE node.uid='OBO_REL:is_a') AND taxon_is_a.object_id = (SELECT node.node_id FROM node WHERE node.uid=?)) ");
        }
        return query.toString();
    }

}
