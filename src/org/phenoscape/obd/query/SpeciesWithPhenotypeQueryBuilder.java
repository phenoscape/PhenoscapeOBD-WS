package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.phenoscape.obd.model.PhenotypeSpec;
import org.phenoscape.obd.model.Vocab.OBO;
import org.phenoscape.obd.model.Vocab.TAXRANK;

public class SpeciesWithPhenotypeQueryBuilder extends QueryBuilder {
    
    final String taxonID;
    final PhenotypeSpec phenotype;

    public SpeciesWithPhenotypeQueryBuilder(String taxonID, PhenotypeSpec phenotype) {
        this.taxonID = taxonID;
        this.phenotype = phenotype;
    }

    @Override
    protected String getQuery() {
        final StringBuffer query = new StringBuffer();
        query.append("SELECT DISTINCT taxon.uid AS taxon_uid FROM asserted_taxon_annotation ");
        query.append(String.format("JOIN taxon ON (taxon.node_id = asserted_taxon_annotation.taxon_node_id AND taxon.rank_uid = '%s') ", TAXRANK.SPECIES));
        query.append(String.format("JOIN link taxon_is_a ON (taxon_is_a.node_id = asserted_taxon_annotation.taxon_node_id AND taxon_is_a.predicate_id = %s AND taxon_is_a.object_id = %s) ", this.node(OBO.IS_A), NODE));
        if (phenotype.getEntityID() != null) {
            if (phenotype.includeEntityParts()) {
                query.append(String.format("JOIN link phenotype_inheres_in_part_of ON (phenotype_inheres_in_part_of.node_id = asserted_taxon_annotation.phenotype_node_id AND phenotype_inheres_in_part_of.predicate_id = %s AND phenotype_inheres_in_part_of.object_id = " + NODE + ") ", this.node(OBO.INHERES_IN_PART_OF)));    
            } else {
                query.append(String.format("JOIN link phenotype_inheres_in ON (phenotype_inheres_in.node_id = asserted_taxon_annotation.phenotype_node_id AND phenotype_inheres_in.predicate_id = %s AND phenotype_inheres_in.object_id = " + NODE + ") ", this.node(OBO.INHERES_IN)));
            }
        }
        if (phenotype.getQualityID() != null) {
            query.append(String.format("JOIN link quality_is_a ON (quality_is_a.node_id = asserted_taxon_annotation.phenotype_node_id AND quality_is_a.predicate_id = %s AND quality_is_a.object_id = " + NODE + ") ", this.node(OBO.IS_A)));
        }
        if (phenotype.getRelatedEntityID() != null) {
            query.append(String.format("JOIN link related_entity_towards ON (related_entity_towards.node_id = asserted_taxon_annotation.phenotype_node_id AND related_entity_towards.predicate_id = %s AND related_entity_towards.object_id = " + NODE + ") ", this.node(OBO.TOWARDS)));
        }
        return query.toString();
    }

    @Override
    protected void fillStatement(PreparedStatement statement) throws SQLException {
        int index = 1;
        statement.setString(index++, this.taxonID);
        if (this.phenotype.getEntityID() != null) {
            statement.setString(index++, this.phenotype.getEntityID());                    
        }
        if (this.phenotype.getQualityID() != null) {
            statement.setString(index++, this.phenotype.getQualityID());
        }
        if (this.phenotype.getRelatedEntityID() != null) {
            statement.setString(index++, this.phenotype.getRelatedEntityID());                    
        }
    }

}
