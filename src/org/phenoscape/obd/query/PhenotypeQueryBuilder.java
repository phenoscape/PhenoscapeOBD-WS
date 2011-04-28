package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.phenoscape.obd.model.PhenotypeSpec;
import org.phenoscape.obd.model.Vocab.OBO;
import org.phenoscape.obd.query.AnnotationsQueryConfig.SORT_COLUMN;

public class PhenotypeQueryBuilder extends QueryBuilder {

    private final AnnotationsQueryConfig config;
    private final boolean totalOnly;
    private static final Map<SORT_COLUMN, String> COLUMNS = new HashMap<SORT_COLUMN, String>();
    static {
        COLUMNS.put(SORT_COLUMN.ENTITY, "entity_node_id");
        COLUMNS.put(SORT_COLUMN.QUALITY, "quality_node_id");
        COLUMNS.put(SORT_COLUMN.RELATED_ENTITY, "related_entity_node_id");
    }


    public PhenotypeQueryBuilder(AnnotationsQueryConfig config, boolean totalOnly) {
        this.config = config;
        this.totalOnly = totalOnly;
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        int index = 1;
        for (String taxonID : this.config.getTaxonIDs()) {
            statement.setString(index++, taxonID);
        }
        if (!this.config.getPhenotypes().isEmpty()) {
            for (PhenotypeSpec phenotype : this.config.getPhenotypes()) {
                if (phenotype.getEntityID() != null) {
                    statement.setString(index++, phenotype.getEntityID());                    
                }
                if (phenotype.getQualityID() != null) {
                    statement.setString(index++, phenotype.getQualityID());
                }
                if (phenotype.getRelatedEntityID() != null) {
                    statement.setString(index++, phenotype.getRelatedEntityID());                    
                }
            }
        }
        if (!this.config.getPublicationIDs().isEmpty()) {
            for (String publicationID : this.config.getPublicationIDs()) {
                statement.setString(index++, publicationID);
            }
        }
        if (!this.config.getGeneIDs().isEmpty()) {
            for (String geneID : this.config.getGeneIDs()) {
                statement.setString(index++, geneID);
            }
        }
        if (!this.config.getGeneClassIDs().isEmpty()) {
            for (String geneClassID : this.config.getGeneClassIDs()) {
                statement.setString(index++, geneClassID);
            }
        }
        if (!this.totalOnly) {
            statement.setInt(index++, this.config.getLimit());
            statement.setInt(index++, this.config.getIndex());
        }
    }

    @Override
    protected String getQuery() {
        final List<String> intersects = new ArrayList<String>();
        if (!this.config.getTaxonIDs().isEmpty()) {
            intersects.add(this.getTaxaQuery(this.config.getTaxonIDs()));
        }
        if (!this.config.getPhenotypes().isEmpty()) {
            intersects.add(this.getPhenotypesQuery(this.config.getPhenotypes()));
        }
        if (!this.config.getPublicationIDs().isEmpty()) {
            intersects.add(this.getPublicationsQuery(this.config.getPublicationIDs()));
        }
        if (!this.config.getGeneIDs().isEmpty()) {
            intersects.add(this.getGenesQuery(this.config.getGeneIDs()));
        }
        if (!this.config.getGeneClassIDs().isEmpty()) {
            intersects.add(this.getGeneClassesQuery(this.config.getGeneClassIDs()));
        }
        final String baseQuery;
        if (intersects.isEmpty()) {
            baseQuery = "SELECT * FROM phenotype ";
        } else {
            baseQuery = "(" + StringUtils.join(intersects, " INTERSECT ") + ") ";
        }
        final String query;
        if (this.totalOnly) {
            query = "SELECT count(*) FROM (" + baseQuery + ") AS query";
        } else {
            query = "SELECT * FROM " + "(" + baseQuery + ") AS query " + this.getJoinText() + "ORDER BY simple_label " + this.getSortText() + "LIMIT ? OFFSET ? " ;
        }
        return query;
    }

    private String getSortText() {
        return this.config.sortDescending() ? "DESC " : ""; 
    }

    private String getTaxaQuery(List<String> taxonIDs) {
        final StringBuffer query = new StringBuffer();
        query.append("(");
        final List<String> subQueries = new ArrayList<String>();
        for (String taxonID : taxonIDs) {
            subQueries.add(this.getTaxonQuery(taxonID));
        }
        query.append(StringUtils.join(subQueries, (this.config.matchAllTaxa() ? " INTERSECT " : " UNION ")));
        query.append(")");
        return query.toString();
    }

    private String getTaxonQuery(String taxonID) {
        final StringBuffer query = new StringBuffer();
        query.append("SELECT * FROM phenotype ");
        query.append("WHERE phenotype.node_id IN ");
        query.append("(");
        query.append("SELECT phenotype_node_id FROM asserted_taxon_annotation ");
        query.append("WHERE asserted_taxon_annotation.taxon_node_id IN ");
        query.append("(");
        query.append(String.format("SELECT taxon.node_id FROM taxon JOIN link taxon_is_a ON (taxon_is_a.node_id = taxon.node_id AND taxon_is_a.predicate_id = %s AND taxon_is_a.object_id =  %s", this.node(OBO.IS_A), NODE));
        query.append(")");
        query.append(")");
        query.append(")");
        return query.toString();
    }

    private String getPhenotypesQuery(List<PhenotypeSpec> phenotypes) {
        final StringBuffer query = new StringBuffer();
        query.append("SELECT * FROM phenotype ");
        query.append("WHERE phenotype.node_id IN ");
        query.append("(");
        final List<String> unions = new ArrayList<String>();
        for (PhenotypeSpec phenotype : phenotypes) {
            unions.add(this.getPhenotypeQuery(phenotype));
        }
        query.append(StringUtils.join(unions, " UNION "));
        query.append(")");        
        return query.toString();
    }

    private String getPhenotypeQuery(PhenotypeSpec phenotype) {
        final StringBuffer query = new StringBuffer();
        query.append("(SELECT phenotype.node_id FROM phenotype ");
        if (phenotype.getEntityID() != null) {
            if (phenotype.includeEntityParts()) {
                query.append(String.format("JOIN link phenotype_inheres_in_part_of ON (phenotype_inheres_in_part_of.node_id = phenotype.node_id AND phenotype_inheres_in_part_of.predicate_id = %s) ", this.node(OBO.INHERES_IN_PART_OF)));    
            } else {
                query.append(String.format("JOIN link phenotype_inheres_in ON (phenotype_inheres_in.node_id = phenotype.node_id AND phenotype_inheres_in.predicate_id = %s) ", this.node(OBO.INHERES_IN)));
            }
        }
        if (phenotype.getQualityID() != null) {
            query.append(String.format("JOIN link quality_is_a ON (quality_is_a.node_id = phenotype.node_id AND quality_is_a.predicate_id = %s) ", this.node(OBO.IS_A)));
        }
        if (phenotype.getRelatedEntityID() != null) {
            query.append(String.format("JOIN link related_entity_towards ON (related_entity_towards.node_id = phenotype.node_id AND related_entity_towards.predicate_id = %s) ", this.node(OBO.TOWARDS)));  
        }
        query.append("WHERE ");
        query.append(this.translate(phenotype));
        query.append(") ");
        return query.toString();
    }

    private String translate(PhenotypeSpec phenotype) {
        final StringBuffer buffer = new StringBuffer();
        buffer.append("(");
        final List<String> terms = new ArrayList<String>();
        if (phenotype.getEntityID() != null) {
            if (phenotype.includeEntityParts()) {
                terms.add("phenotype_inheres_in_part_of.object_id = " + NODE + " ");    
            } else {
                terms.add("phenotype_inheres_in.object_id = " + NODE + " ");
            }
        }
        if (phenotype.getQualityID() != null) {
            terms.add("quality_is_a.object_id = " + NODE + " ");
        }
        if (phenotype.getRelatedEntityID() != null) {
            terms.add("related_entity_towards.object_id = " + NODE + " ");
        }
        buffer.append(StringUtils.join(terms, " AND "));
        buffer.append(")");
        return buffer.toString();
    }

    private String getPublicationsQuery(List<String> publicationIDs) {
        final StringBuffer query = new StringBuffer();
        query.append("(");
        final List<String> subQueries = new ArrayList<String>();
        for (String publicationID : publicationIDs) {
            subQueries.add(this.getPublicationQuery(publicationID));
        }
        query.append(StringUtils.join(subQueries, (this.config.matchAllPublications() ? " INTERSECT " : " UNION ")));
        query.append(")");
        return query.toString();
    }

    private String getPublicationQuery(String publicationID) {
        final StringBuffer query = new StringBuffer();
        query.append("(");
        query.append(" SELECT * FROM phenotype ");
        query.append(" WHERE phenotype.node_id IN ");
        query.append("(");
        query.append("SELECT phenotype_node_id FROM asserted_taxon_annotation ");
        query.append("JOIN annotation_source ON (annotation_source.annotation_id = asserted_taxon_annotation.annotation_id ) ");
        query.append(String.format(" WHERE annotation_source.publication_node_id = %s ", NODE));
        query.append(")");
        query.append(") ");
        return query.toString();
    }

    private String getGenesQuery(List<String> genes) {
        final StringBuffer query = new StringBuffer();
        query.append("(");
        final List<String> subQueries = new ArrayList<String>();
        for (String geneID : genes) {
            subQueries.add(this.getGeneQuery(geneID));
        }
        query.append(StringUtils.join(subQueries, (this.config.matchAllGenes() ? " INTERSECT " : " UNION ")));
        query.append(")");
        return query.toString();
    }

    private String getGeneQuery(String geneID) {
        final StringBuffer query = new StringBuffer();
        query.append("(");
        query.append(" SELECT * FROM phenotype ");
        query.append(" WHERE phenotype.node_id IN ");
        query.append("(SELECT distinct_gene_annotation.phenotype_node_id FROM distinct_gene_annotation WHERE ");
        query.append("distinct_gene_annotation.gene_uid = ? ");
        query.append(")");
        query.append(")");
        return query.toString();
    }
    
    private String getGeneClassesQuery(List<String> geneClasses) {
        final StringBuffer query = new StringBuffer();
        query.append("(");
        final List<String> subQueries = new ArrayList<String>();
        for (String geneID : geneClasses) {
            subQueries.add(this.getGeneClassQuery(geneID));
        }
        query.append(StringUtils.join(subQueries, (this.config.matchAllGeneClasses() ? " INTERSECT " : " UNION ")));
        query.append(")");
        return query.toString();
    }
    
    private String getGeneClassQuery(String geneClassID) {
        final StringBuffer query = new StringBuffer();
        query.append("(");
        query.append(" SELECT * FROM phenotype ");
        query.append(" WHERE phenotype.node_id IN ");
        query.append("(SELECT distinct_gene_annotation.phenotype_node_id FROM distinct_gene_annotation ");
        query.append(String.format(" JOIN link gene_class_link ON (gene_class_link.predicate_id IN (SELECT node_id FROM node WHERE uid IN ('%s', '%s', '%s')) AND gene_class_link.node_id = distinct_gene_annotation.gene_node_id AND gene_class_link.object_id = %s) ", OBO.HAS_FUNCTION, OBO.LOCATED_IN, OBO.PARTICIPATES_IN, NODE));
        query.append(")");
        query.append(")");
        return query.toString();
    }

    private String getJoinText() {
        return String.format("JOIN smart_node_label ON (smart_node_label.node_id = %s) ", COLUMNS.get(this.config.getSortColumn()));
    }

}
