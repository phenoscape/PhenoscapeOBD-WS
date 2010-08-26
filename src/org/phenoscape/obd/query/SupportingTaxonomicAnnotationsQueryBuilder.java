package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.phenoscape.obd.model.Vocab.OBO;

public class SupportingTaxonomicAnnotationsQueryBuilder extends QueryBuilder {

    private final AnnotationsQueryConfig config;
    //private static final String TABLE = "annotation_source";

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
        query.append("SELECT taxon.node_id AS taxon_node_id, taxon.uid AS taxon_uid, taxon.label AS taxon_label, taxon.rank_uid AS taxon_rank_uid, taxon.rank_label AS taxon_rank_label, taxon.is_extinct AS taxon_is_extinct, phenotype.entity_uid, phenotype.entity_label, phenotype.quality_uid, phenotype.quality_label, phenotype.related_entity_uid, phenotype.related_entity_label, publication.uid AS publication_uid, publication.label AS publication_label, otu.uid AS otu_uid, otu.label AS otu_label, character.label AS character_label, character.character_number, state.label AS state_label FROM annotation_source ");
        query.append(" JOIN taxon_annotation ON (taxon_annotation.annotation_id = annotation_source.annotation_id) ");
        query.append(" JOIN phenotype ON (taxon_annotation.phenotype_node_id = phenotype.node_id) ");
        query.append(" JOIN taxon ON (taxon_annotation.taxon_node_id = taxon.node_id) ");
        query.append(" JOIN node publication ON (publication.node_id = annotation_source.publication_node_id) ");
        query.append(" JOIN otu ON (otu.node_id = annotation_source.otu_node_id) ");
        query.append(" JOIN character ON (character.node_id = annotation_source.character_node_id) ");
        query.append(" JOIN state ON (state.node_id = annotation_source.state_node_id) ");
        final String isANode = String.format(NODE_S, OBO.IS_A);
        if (this.config.includeInferredAnnotations()) {
            query.append(String.format(" JOIN link taxon_is_a ON (taxon_is_a.object_id = taxon_annotation.taxon_node_id AND taxon_is_a.predicate_id = %s AND taxon_is_a.node_id = %s) ", isANode, NODE));    
        }        
        query.append(" WHERE ");
        final List<String> wheres = new ArrayList<String>();
        if (!this.config.includeInferredAnnotations()) {
            wheres.add(String.format(" taxon_annotation.taxon_node_id = %s ", NODE));
        }
        wheres.add(" phenotype.entity_uid = ? ");
        wheres.add(" phenotype.quality_uid = ? ");
        if (this.config.getPhenotypes().get(0).getRelatedEntityID() != null) {
            wheres.add(" phenotype.related_entity_uid = ? ");
        }
        if (!this.config.getPublicationIDs().isEmpty()) {
            final StringBuffer pubQuery = new StringBuffer();
            pubQuery.append(" annotation_source.publication_node_id IN ");
            pubQuery.append("(");
            pubQuery.append(String.format(" SELECT node_id from node WHERE uid IN %s", this.createPlaceholdersList(this.config.getPublicationIDs().size())));
            pubQuery.append(")");
            wheres.add(pubQuery.toString());

        }
        query.append(StringUtils.join(wheres, " AND "));        
        final String mainQuery = "(" + query.toString() + ")";
        final String exactQuery = "(" + this.getTempExactTaxonQuery() + ")";
        final String union = mainQuery + " UNION " + exactQuery;
        return union;
    }

    private String getTempExactTaxonQuery() {
        final StringBuffer query = new StringBuffer();
        query.append("SELECT taxon.node_id AS taxon_node_id, taxon.uid AS taxon_uid, taxon.label AS taxon_label, taxon.rank_uid AS taxon_rank_uid, taxon.rank_label AS taxon_rank_label, taxon.is_extinct AS taxon_is_extinct, phenotype.entity_uid, phenotype.entity_label, phenotype.quality_uid, phenotype.quality_label, phenotype.related_entity_uid, phenotype.related_entity_label, publication.uid AS publication_uid, publication.label AS publication_label, otu.uid AS otu_uid, otu.label AS otu_label, character.label AS character_label, character.character_number, state.label AS state_label FROM annotation_source "); //TODO
        query.append(" JOIN taxon_annotation ON (taxon_annotation.annotation_id = annotation_source.annotation_id) ");
        query.append(" JOIN phenotype ON (taxon_annotation.phenotype_node_id = phenotype.node_id) ");
        query.append(" JOIN taxon ON (taxon_annotation.taxon_node_id = taxon.node_id) ");
        query.append(" JOIN node publication ON (publication.node_id = annotation_source.publication_node_id) ");
        query.append(" JOIN otu ON (otu.node_id = annotation_source.otu_node_id) ");
        query.append(" JOIN character ON (character.node_id = annotation_source.character_node_id) ");
        query.append(" JOIN state ON (state.node_id = annotation_source.state_node_id) ");
        query.append(" WHERE ");
        final List<String> wheres = new ArrayList<String>();
        wheres.add(String.format(" taxon_annotation.taxon_node_id = %s ", NODE));
        wheres.add(" phenotype.entity_uid = ? ");
        wheres.add(" phenotype.quality_uid = ? ");
        if (this.config.getPhenotypes().get(0).getRelatedEntityID() != null) {
            wheres.add(" phenotype.related_entity_uid = ? ");
        }
        if (!this.config.getPublicationIDs().isEmpty()) {
            final StringBuffer pubQuery = new StringBuffer();
            pubQuery.append(" annotation_source.publication_node_id IN ");
            pubQuery.append("(");
            pubQuery.append(String.format(" SELECT node_id from node WHERE uid IN %s", this.createPlaceholdersList(this.config.getPublicationIDs().size())));
            pubQuery.append(")");
            wheres.add(pubQuery.toString());

        }
        query.append(StringUtils.join(wheres, " AND "));
        return query.toString();
    }

}
