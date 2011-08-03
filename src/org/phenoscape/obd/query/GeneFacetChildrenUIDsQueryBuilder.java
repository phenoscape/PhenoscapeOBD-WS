package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.phenoscape.obd.model.Vocab.OBO;

public class GeneFacetChildrenUIDsQueryBuilder extends QueryBuilder {
    
    final String termUID;
    final boolean returnGenes;

    public GeneFacetChildrenUIDsQueryBuilder(String termUID, boolean returnGenes) {
        this.termUID = termUID;
        this.returnGenes = returnGenes;
    }

    @Override
    protected String getQuery() {
        if (this.returnGenes) {
            return this.getGeneQuery();
        } else {
            return this.getGOQuery();
        }
    }
    
    private String getGOQuery() {
        final StringBuilder query = new StringBuilder();
        query.append("SELECT subject.uid AS child_uid ");
        query.append("FROM link ");
        query.append("JOIN node subject ON (subject.node_id = link.node_id) ");
        query.append(String.format("WHERE link.predicate_id IN (SELECT node_id FROM node WHERE uid = '%s') AND link.object_id = %s AND link.is_inferred = false", OBO.IS_A, NODE));
        return query.toString();
    }
    
    private String getGeneQuery() {
        final StringBuilder query = new StringBuilder();
        query.append("SELECT subject.uid AS child_uid ");
        query.append("FROM link ");
        query.append("JOIN node subject ON (subject.node_id = link.node_id) ");
        //only return gene children that have annotations in the database - this is a crude optimization; the calling code could possibly have even passed in
        //the required phenotype, but that introduces more complication than needed
        query.append("JOIN gene_annotation ON (gene_annotation.gene_node_id = subject.node_id) ");
        query.append(String.format("WHERE link.predicate_id IN (SELECT node_id FROM node WHERE uid IN ('%s', '%s', '%s')) AND link.object_id = %s AND link.is_inferred = false", OBO.HAS_FUNCTION, OBO.LOCATED_IN, OBO.PARTICIPATES_IN, NODE));
        return query.toString();
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        statement.setString(1, this.termUID);
    }

}
