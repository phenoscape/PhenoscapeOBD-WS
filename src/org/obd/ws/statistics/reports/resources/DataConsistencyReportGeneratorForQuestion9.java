package org.obd.ws.statistics.reports.resources;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public class DataConsistencyReportGeneratorForQuestion9 extends AbstractStatisticalReportGenerator{

    public final String QUERY_FOR_ALL_STATES_FOR_ALL_CHARACTERS = 
        "SELECT DISTINCT " +
        "character_node.node_id AS _character, " +
        "state_node.node_id AS state " +
        "FROM " +
        "node AS character_node " +
        "JOIN (link AS has_state_link " +
        "JOIN (node AS state_node " +
        "JOIN (link AS has_phenotype_link " +
        "JOIN node AS phenotype_node " +
        "ON (phenotype_node.node_id = has_phenotype_link.object_id AND " +
        "	has_phenotype_link.predicate_id = (SELECT node_id FROM node WHERE uid = 'cdao:has_Phenotype'))) " +
        "ON (has_phenotype_link.node_id	= state_node.node_id)) " +
        "ON (state_node.node_id = has_state_link.object_id AND " +
        "	has_state_link.predicate_id = (SELECT node_id FROM node WHERE uid = 'cdao:has_Datum'))) " +
        "ON (has_state_link.node_id = character_node.node_id)";

    public final String QUERY_FOR_CHARACTER_WITH_ONLY_ONE_ANNOTATED_STATE = 
        "SELECT " +
        "character_node.label AS char_label, " +
        "pub.authors AS authors, " +
        "pub.publication_year AS pub_year, " +
        "_number.val AS char_number, " +
        "tagval.val AS char_comment " +
        "FROM " +
        "node AS character_node " +
        "LEFT OUTER JOIN tagval " +
        "ON (tagval.node_id = character_node.node_id AND " +
        "	tagval.tag_id = (SELECT node_id FROM node WHERE uid = 'PHENOSCAPE:has_comment')) " +
        "LEFT OUTER JOIN tagval AS _number " +
        "ON (_number.node_id = character_node.node_id AND " +
        "	_number.tag_id = (SELECT node_id FROM node WHERE uid = 'PHENOSCAPE:has_number')) " +
        "JOIN (link AS ds_link " +
        "JOIN (link AS has_publication_link " +
        "JOIN (node AS pub_node " +
        "JOIN dw_publication_table AS pub " +
        "ON (pub.publication = pub_node.uid)) " +
        "ON (pub_node.node_id = has_publication_link.object_id AND " +
        "	has_publication_link.predicate_id = (SELECT node_ID FROM node WHERE uid = 'PHENOSCAPE:has_publication'))) " +
        "ON (has_publication_link.node_id = ds_link.node_id AND " +
        "	ds_link.predicate_id = (SELECT node_id FROM node WHERE uid = 'cdao:has_Character'))) " +
        "ON (ds_link.object_id = character_node.node_id) " +
        "WHERE " +
        "character_node.node_id = ?";

    public final String TITLE_LINE = "CHARACTER\tPUBLICATION\tCHAR_NUMBER\tCOMMENT\n";

    private Connection conn;
    private String lines;
    private Map<Integer, Set<Integer>> mapFromCharactersToAnnotatedStates;

    @Get("json")
    public Representation getTextRepresentation() throws ResourceException {
        try {
            connectShardToDatabase();
            conn = this.getShard().getConnection();
            mapFromCharactersToAnnotatedStates = new HashMap<Integer, Set<Integer>>();
            lines = this.findCharactersWithOnlyOneAnnotatedState();
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

    public String findCharactersWithOnlyOneAnnotatedState() {
        int ct = 0;
        String line = "";
        this.populateMapFromCharacterToAnnotatedStates();
        for (Integer character : this.mapFromCharactersToAnnotatedStates
                .keySet()) {
            if (this.mapFromCharactersToAnnotatedStates.get(character).size() == 1) {
                ++ct;
                line += printCharacterWithOnlyOneAnnotatedState(character)
                + "\n";
            }
        }
        if (ct > 0) {
            line =  TITLE_LINE + line;
        }
        return line;
    }

    private String printCharacterWithOnlyOneAnnotatedState(Integer charID) {

        String label, publication, number, comment, line = "";

        try {
            PreparedStatement ps = conn
            .prepareStatement(QUERY_FOR_CHARACTER_WITH_ONLY_ONE_ANNOTATED_STATE);
            ps.setInt(1, charID);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                label = rs.getString(1);
                publication = rs.getString(2) + ", " + rs.getString(3);
                number = rs.getString(4);
                comment = rs.getString(5);
                comment = (comment == null || comment.trim().length() == 0 ? ""
                        : comment);
                line = label + "\t" + publication + "\t" + number + "\t"
                + comment;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return line;
    }

    private void populateMapFromCharacterToAnnotatedStates() {

        Set<Integer> annotatedStateSet;
        Integer character, state;

        try {
            PreparedStatement ps = conn
            .prepareStatement(QUERY_FOR_ALL_STATES_FOR_ALL_CHARACTERS);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                character = rs.getInt(1);
                state = rs.getInt(2);
                if (this.mapFromCharactersToAnnotatedStates.keySet().contains(
                        character))
                    annotatedStateSet = this.mapFromCharactersToAnnotatedStates
                    .get(character);
                else
                    annotatedStateSet = new HashSet<Integer>();
                annotatedStateSet.add(state);
                this.mapFromCharactersToAnnotatedStates.put(character,
                        annotatedStateSet);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
