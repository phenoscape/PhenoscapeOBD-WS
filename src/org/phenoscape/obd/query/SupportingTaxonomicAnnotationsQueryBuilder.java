package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.phenoscape.obd.model.Vocab.OBO;

public class SupportingTaxonomicAnnotationsQueryBuilder extends QueryBuilder {

    private final AnnotationsQueryConfig config;
    private static final String TABLE = "queryable_taxon_annotation";

    public SupportingTaxonomicAnnotationsQueryBuilder(AnnotationsQueryConfig config) {
        this.config = config;
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        int index = 1;
        statement.setString(index++, this.config.getTaxonIDs().get(0));
        statement.setString(index++, this.config.getPhenotypes().get(0).getEntityID());
        statement.setString(index++, this.config.getPhenotypes().get(0).getQualityID());
        if (this.config.getPhenotypes().get(0).getRelatedEntityID() != null) {
            statement.setString(index++, this.config.getPhenotypes().get(0).getRelatedEntityID());
        }
        for (String publicationID : this.config.getPublicationIDs()) {
            statement.setString(index++, publicationID);
        }
        //these are for the extra exact query and are temporary -- this is due to a problem in OBD with annotated taxa not having reflexive is_a links 
        statement.setString(index++, this.config.getTaxonIDs().get(0));
        statement.setString(index++, this.config.getPhenotypes().get(0).getEntityID());
        statement.setString(index++, this.config.getPhenotypes().get(0).getQualityID());
        if (this.config.getPhenotypes().get(0).getRelatedEntityID() != null) {
            statement.setString(index++, this.config.getPhenotypes().get(0).getRelatedEntityID());
        }
        for (String publicationID : this.config.getPublicationIDs()) {
            statement.setString(index++, publicationID);
        }
    }

    @Override
    protected String getQuery() {
        final StringBuffer query = new StringBuffer();
        query.append(String.format("SELECT DISTINCT %s.* FROM %s ", TABLE, TABLE));
        final String isANode = String.format(NODE_S, OBO.IS_A);
        if (this.config.includeInferredAnnotations()) {
            query.append(String.format(" JOIN link taxon_is_a ON (taxon_is_a.object_id = %s.taxon_node_id AND taxon_is_a.predicate_id = %s AND taxon_is_a.node_id = %s) ", TABLE, isANode, NODE));    
        }        
        query.append(" WHERE ");
        final List<String> wheres = new ArrayList<String>();
        if (!this.config.includeInferredAnnotations()) {
            wheres.add(String.format(" %s.taxon_uid = ? ", TABLE));
        }
        wheres.add(String.format(" %s.entity_uid = ? ", TABLE));
        wheres.add(String.format(" %s.quality_uid = ? ", TABLE));
        if (this.config.getPhenotypes().get(0).getRelatedEntityID() != null) {
            wheres.add(String.format(" %s.related_entity_uid = ? ", TABLE));
        }
        if (!this.config.getPublicationIDs().isEmpty()) {
            wheres.add(String.format(" %s.publication_uid IN %s", TABLE, this.createPlaceholdersList(this.config.getPublicationIDs().size())));
        }
        wheres.add(String.format(" %s.is_inferred = false ", TABLE));
        query.append(StringUtils.join(wheres, " AND "));        
        final String mainQuery = "(" + query.toString() + ")";
        final String exactQuery = "(" + this.getTempExactTaxonQuery() + ")";
        final String union = mainQuery + " UNION " + exactQuery;
        return union;
    }
    
    private String getTempExactTaxonQuery() {
        final StringBuffer query = new StringBuffer();
        query.append(String.format("SELECT DISTINCT %s.* FROM %s ", TABLE, TABLE));
        query.append(" WHERE ");
        final List<String> wheres = new ArrayList<String>();
        wheres.add(String.format(" %s.taxon_uid = ? ", TABLE));
        wheres.add(String.format(" %s.entity_uid = ? ", TABLE));
        wheres.add(String.format(" %s.quality_uid = ? ", TABLE));
        if (this.config.getPhenotypes().get(0).getRelatedEntityID() != null) {
            wheres.add(String.format(" %s.related_entity_uid = ? ", TABLE));
        }
        if (!this.config.getPublicationIDs().isEmpty()) {
            wheres.add(String.format(" %s.publication_uid IN %s", TABLE, this.createPlaceholdersList(this.config.getPublicationIDs().size())));
        }
        wheres.add(String.format(" %s.is_inferred = false ", TABLE));
        query.append(StringUtils.join(wheres, " AND "));
        return query.toString();
    }

    private Logger log() {
        return Logger.getLogger(this.getClass());
    }
}
