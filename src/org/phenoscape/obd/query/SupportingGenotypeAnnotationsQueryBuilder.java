package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class SupportingGenotypeAnnotationsQueryBuilder extends QueryBuilder {

    private final AnnotationsQueryConfig config;
    private static final String TABLE = "queryable_gene_annotation";

    public SupportingGenotypeAnnotationsQueryBuilder(AnnotationsQueryConfig config) {
        this.config = config;
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        int index = 1;
        statement.setString(index++, this.config.getGeneIDs().get(0));
        statement.setString(index++, this.config.getPhenotypes().get(0).getEntityID());
        statement.setString(index++, this.config.getPhenotypes().get(0).getQualityID());
        if (this.config.getPhenotypes().get(0).getRelatedEntityID() != null) {
            statement.setString(index++, this.config.getPhenotypes().get(0).getRelatedEntityID());
        }
    }

    @Override
    protected String getQuery() {
        final StringBuffer query = new StringBuffer();
        query.append(String.format("SELECT DISTINCT %s.* FROM %s ", TABLE, TABLE));
        query.append(" WHERE ");
        final List<String> wheres = new ArrayList<String>();
        wheres.add(String.format(" %s.gene_uid = ? ", TABLE));
        wheres.add(String.format(" %s.entity_uid = ? ", TABLE));
        wheres.add(String.format(" %s.quality_uid = ? ", TABLE));
        if (this.config.getPhenotypes().get(0).getRelatedEntityID() != null) {
            wheres.add(String.format(" %s.related_entity_uid = ? ", TABLE));
        }        
        query.append(StringUtils.join(wheres, " AND "));
        return query.toString();
    }

    private Logger log() {
        return Logger.getLogger(this.getClass());
    }
}
