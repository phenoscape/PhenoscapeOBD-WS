package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MatrixDataQueryBuilder extends QueryBuilder {

    final String publicationID;

    public MatrixDataQueryBuilder(String pubID) {
        this.publicationID = pubID;
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        statement.setString(1, this.publicationID);
    }

    @Override
    protected String getQuery() {
        final StringBuffer query = new StringBuffer();
        query.append("SELECT otu.node_id as otu_node_id, otu.uid as otu_uid, otu.label as otu_label, otu.comment as otu_comment, taxon.uid as taxon_uid, taxon.label as taxon_label, taxon.rank_uid, taxon.rank_label, taxon.is_extinct, character.character_number, character.uid as character_uid, character.label as character_label, state.uid as state_uid, state.label as state_label ");
        query.append("FROM annotation_source ");
        query.append("JOIN otu on (annotation_source.otu_node_id = otu.node_id)" );
        query.append("JOIN taxon on (taxon.node_id = otu.taxon_node_id) ");
        query.append("JOIN character on (character.node_id = annotation_source.character_node_id) ");
        query.append("JOIN state on (state.node_id = annotation_source.state_node_id) ");
        query.append("WHERE annotation_source.publication_node_id = (select node_id from node where uid=?) ");
        return query.toString();
    }

}
