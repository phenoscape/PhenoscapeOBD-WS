package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.phenoscape.obd.model.Vocab.OBO;

public class TermQueryBuilder extends QueryBuilder {

    final String uid;

    public TermQueryBuilder(String uid) {
        this.uid = uid;
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        statement.setString(1, this.uid);
    }

    @Override
    protected String getQuery() {
        return  "SELECT term.*, description.label AS definition, tagval.val AS comment, source.uid AS source_uid, source.label AS source_label " +
        "FROM node term " +
        "LEFT OUTER JOIN description ON (description.node_id = term.node_id) " +
        "LEFT OUTER JOIN node comment_rel ON (comment_rel.uid = '" + OBO.COMMENT + "') " +
        "LEFT OUTER JOIN tagval ON (tagval.tag_id = comment_rel.node_id AND tagval.node_id = term.node_id) " +
        "LEFT OUTER JOIN node source ON (source.node_id = term.source_id) " +
        "WHERE term.uid = ?";
    }

}
