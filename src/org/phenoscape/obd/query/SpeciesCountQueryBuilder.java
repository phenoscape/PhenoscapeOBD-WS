package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.phenoscape.obd.model.Vocab.OBO;
import org.phenoscape.obd.model.Vocab.TAXRANK;

public class SpeciesCountQueryBuilder extends QueryBuilder {
    
    final String taxonUID;

    public SpeciesCountQueryBuilder(String uid) {
        this.taxonUID = uid;
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        statement.setString(1, this.taxonUID);
    }

    @Override
    protected String getQuery() {
        final StringBuffer query = new StringBuffer();
        query.append("SELECT count(*) as species_count FROM taxon parent ");
        query.append(String.format("JOIN link is_a_link ON (is_a_link.predicate_id = %s AND is_a_link.object_id = parent.node_id) ", this.node(OBO.IS_A)));
        query.append(String.format("JOIN taxon child ON (child.node_id = is_a_link.node_id AND child.rank_uid = '%s') ", TAXRANK.SPECIES));
        query.append(String.format("WHERE parent.uid = ? AND (parent.rank_uid != '%s' OR parent.rank_uid IS NULL) ", TAXRANK.SPECIES));
        return query.toString();
    }

}
