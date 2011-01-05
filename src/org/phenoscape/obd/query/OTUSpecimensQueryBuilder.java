package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.phenoscape.obd.model.Vocab.DWC;

public class OTUSpecimensQueryBuilder extends QueryBuilder {

    final String otuID;

    public OTUSpecimensQueryBuilder(String otuID) {
        this.otuID = otuID;
    }

    @Override
    protected String getQuery() {
        final StringBuffer query = new StringBuffer();
        query.append("SELECT specimen.*, catalog_id.val AS catalog_id, collection.uid AS collection_uid, collection.label AS collection_label FROM node specimen ");
        query.append(String.format("JOIN link otu_link ON (otu_link.object_id = specimen.node_id AND otu_link.predicate_id = %s) ", this.node(DWC.INDIVIDUAL_ID)));
        query.append(String.format("JOIN link collection_link ON (collection_link.node_id = specimen.node_id AND collection_link.predicate_id = %s) ", this.node(DWC.COLLECTION_ID)));
        query.append(String.format("JOIN tagval catalog_id ON (catalog_id.tag_id = %s AND catalog_id.node_id = specimen.node_id) ", this.node(DWC.CATALOG_ID)));
        query.append("JOIN node collection ON (collection.node_id = collection_link.object_id) ");
        query.append(String.format("WHERE otu_link.node_id = %s", NODE));
        return query.toString();
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        statement.setString(1, this.otuID);
    }

}
