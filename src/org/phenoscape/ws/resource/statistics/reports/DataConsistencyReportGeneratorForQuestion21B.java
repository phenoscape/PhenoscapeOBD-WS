package org.phenoscape.ws.resource.statistics.reports;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public class DataConsistencyReportGeneratorForQuestion21B extends AbstractStatisticalReportGenerator{

    private Connection conn;

    public static final String
    QUERY_FOR_PHENOTYPES_WITH_NON_RELATIONAL_QUALITIES_HAVING_RELATED_ENTITIES = 
        "SELECT DISTINCT " +
        "_character.quality_label AS attribute," +
        " entity1.entity_label AS primaryEntity, " +
        "quality.quality_label AS quality, " +
        "entity2.entity_label AS related_entity," +
        " pub_dw.authors AS authors, " +
        "pub_dw.publication_year AS year, " +
        "_number.val AS characterNumber," +
        " character_node.label AS character " +
        "FROM " +
        "dw_phenotype_table AS phenotype " +
        "JOIN dw_entity_table AS entity1 " +
        "ON (entity1.entity_nid = phenotype.inheres_in_entity_nid) " +
        "LEFT OUTER JOIN dw_entity_table AS entity2 " +
        "ON (entity2.entity_nid = phenotype.towards_entity_nid) " +
        "JOIN dw_quality_table AS quality " +
        "ON (quality.quality_nid = phenotype.is_a_quality_nid) " +
        "JOIN dw_quality_table AS _character " +
        "ON (_character.quality_nid = phenotype.character_nid) " +
        "JOIN (dw_taxon_phenotype_table AS tp " +
        "JOIN (dw_publication_reif_id_table AS publication " +
        "JOIN dw_publication_table AS pub_dw " +
        "ON (pub_dw.publication = publication.publication)) " +
        "ON (publication.reif_id = tp.reif_id)) " +
        "ON (tp.phenotype_nid = phenotype.phenotype_nid) " +
        "JOIN (link AS has_phenotype_link " +
        "JOIN (link AS has_state_link " +
        "JOIN node AS character_node " +
        "ON (has_state_link.node_id = character_node.node_id)) " +
        "ON (has_phenotype_link.node_id = has_state_link.object_id AND " +
        "	has_state_link.predicate_id = (SELECT node_id FROM node WHERE uid = 'cdao:has_Datum'))) " +
        "ON (phenotype.phenotype_nid = has_phenotype_link.object_id AND " +
        "	has_phenotype_link.predicate_id = (SELECT node_id FROM node WHERE uid = 'cdao:has_Phenotype')) " +
        "LEFT OUTER JOIN tagval AS _number " +
        "ON (_number.node_id = character_node.node_id AND " +
        "	_number.tag_id = (SELECT node_id FROM node WHERE uid = 'PHENOSCAPE:has_number')) " +
        "WHERE " +
        "phenotype.character_nid NOT IN " +
        "(SELECT node_id FROM node WHERE uid IN (?, ?, ?, ?)) AND " +
        "phenotype.towards_entity_nid IS NOT NULL ";									

    public final String TITLE_LINE = 
        "ATTRIBUTE\tPRIMARY ENTITY\tQUALITY\tRELATED ENTITY\tPUBLICATION\tCHARACTER NUMBER\tLABEL\n";

    @Get("txt")
    public Representation getTextRepresentation() throws ResourceException {
        try {
            connectShardToDatabase();
            conn = this.getShard().getConnection();
            String lines = getPhenotypesWithNonRelationalQualitiesHavingRelatedEntities();
            return new StringRepresentation(lines);
        } catch (SQLException sqle) {
            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, "[SQL EXCEPTION] Something broke server side. Consult server logs");
        } catch (ClassNotFoundException e) {
            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, "[CLASS NOT FOUND EXCEPTION] Something broke server side. Consult server logs");
        } finally {
            disconnectShardFromDatabase();
        }
        return null;
    }

    private String getPhenotypesWithNonRelationalQualitiesHavingRelatedEntities() {
        int ct = 0;
        String line = "", attribute, entity1, entity2, quality, authors, year, number, character, publication;
        try {
            PreparedStatement ps = conn
            .prepareStatement(QUERY_FOR_PHENOTYPES_WITH_NON_RELATIONAL_QUALITIES_HAVING_RELATED_ENTITIES);
            ps.setString(1, this.RELATIONAL_SHAPE_QUALITY_IDENTIFIER);
            ps.setString(2, this.RELATIONAL_SPATIAL_QUALITY_IDENTIFIER);
            ps.setString(3, this.RELATIONAL_STRUCTURAL_QUALITY_IDENTIFIER);
            ps.setString(4, this.SIZE_IDENTIFIER);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                ++ct;
                attribute = rs.getString(1);
                entity1 = rs.getString(2);
                quality = rs.getString(3);
                entity2 = rs.getString(4);
                authors = rs.getString(5);
                year = rs.getString(6);
                number = rs.getString(7);
                character = rs.getString(8);
                publication = authors + ", " + year;
                line += attribute + "\t" + entity1 + "\t" + quality + "\t"
                + entity2 + "\t" + publication + "\t" + number + "\t"
                + character + "\n";
            }
            if (ct > 0) {
                line = TITLE_LINE + line;
                return line; 
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }	
        return "No data";
    }

}
