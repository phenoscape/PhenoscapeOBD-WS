package org.phenoscape.obd.sparql;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.phenoscape.obd.query.QueryBuilder;

public class SPARQLLinkQueryBuilder extends QueryBuilder {

    final String subjectUID;
    final String predicateUID;
    final String objectUID;

    public SPARQLLinkQueryBuilder(String subjectUID, String predicateUID, String objectUID) {
        this.subjectUID = subjectUID;
        this.predicateUID = predicateUID;
        this.objectUID = objectUID;
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        int index = 1;
        if (this.subjectUID != null) {
            statement.setString(index++, this.subjectUID);
        }
        if (this.predicateUID != null) {
            statement.setString(index++, this.predicateUID);
        }
        if (this.objectUID != null) {
            statement.setString(index++, this.objectUID);
        }
    }

    @Override
    protected String getQuery() {
        final StringBuffer query = new StringBuffer();
        query.append(" SELECT subject.uid AS subject_uid, predicate.uid AS predicate_uid, obj.uid AS object_uid from link ");
        query.append(" JOIN node subject ON (link.node_id = subject.node_id) ");    
        query.append(" JOIN node predicate ON (link.predicate_id = predicate.node_id) ");
        query.append(" JOIN node obj ON (link.object_id = obj.node_id) ");
        query.append(" WHERE ");
        final List<String> wheres = new ArrayList<String>();
        if (this.subjectUID != null) {
            wheres.add("subject.uid = ?");
        }
        if (this.predicateUID != null) {
            wheres.add("predicate.uid = ?");
        }
        if (this.objectUID != null) {
            wheres.add("obj.uid = ?");
        }
        query.append(StringUtils.join(wheres, " AND "));
        return query.toString();
    }

}
