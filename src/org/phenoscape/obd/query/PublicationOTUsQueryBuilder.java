package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.phenoscape.obd.model.Vocab.CDAO;
import org.phenoscape.obd.model.Vocab.PHENOSCAPE;

public class PublicationOTUsQueryBuilder extends QueryBuilder {
    
    final String publicationID;
    
    public PublicationOTUsQueryBuilder(String pubID) {
        this.publicationID = pubID;
    }

    @Override
    protected String getQuery() {
        final StringBuffer query = new StringBuffer();
        query.append("SELECT * FROM node otu ");
        query.append(String.format("JOIN link matrix_link ON (matrix_link.object_id = otu.node_id AND matrix_link.predicate_id = %s) ", this.node(CDAO.HAS_OTU)));
        query.append(String.format("JOIN link pub_link ON (pub_link.node_id = matrix_link.node_id AND pub_link.predicate_id = %s AND pub_link.object_id = %s) ", this.node(PHENOSCAPE.HAS_PUBLICATION), NODE));
        query.append(String.format("JOIN link taxon_link ON (taxon_link.node_id = otu.node_id AND taxon_link.predicate_id = %s) ", this.node(PHENOSCAPE.HAS_TAXON)));
        query.append("JOIN taxon ON (taxon.node_id = taxon_link.object_id)");
        return query.toString();
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        statement.setString(1, this.publicationID);
    }

}
