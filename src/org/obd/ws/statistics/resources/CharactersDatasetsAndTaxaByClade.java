package org.obd.ws.statistics.resources;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;
import org.obd.model.Node;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public class CharactersDatasetsAndTaxaByClade extends AbstractStatisticsGenerator {

    private Connection conn;

    private JSONObject jObj;
    private List<JSONObject> listOfCladeObjs;

    private final String queryForCountOfCharactersUnderClade = 
        "SELECT DISTINCT " +
        "character_node.uid AS character_uid " +
        "FROM " +
        "node AS phenotype_node " +
        "JOIN link AS exhibits_link ON (phenotype_node.node_id = exhibits_link.object_id) " +
        "JOIN node AS taxon_node ON (exhibits_link.node_id = taxon_node.node_id) " +
        "JOIN link AS is_a_link ON (is_a_link.node_id = taxon_node.node_id) " +
        "JOIN node AS supertaxon_node ON (supertaxon_node.node_id = is_a_link.object_id) " +
        "JOIN link AS cell_to_state_link1 ON (exhibits_link.reiflink_node_id = cell_to_state_link1.node_id) " +
        "JOIN link AS cell_to_state_link2 ON (cell_to_state_link1.object_id = cell_to_state_link2.node_id) " +
        "JOIN link AS has_state_link ON (cell_to_state_link2.object_id = has_state_link.object_id) " +
        "JOIN node AS character_node ON (has_state_link.node_id = character_node.node_id) " +
        "WHERE " +
        "has_state_link.predicate_id = (SELECT node_id FROM node WHERE uid = 'cdao:has_Datum') AND " +
        "cell_to_state_link1.predicate_id = (SELECT node_id FROM node WHERE uid = 'cdao:has_State') AND " +
        "cell_to_state_link2.predicate_id = (SELECT node_id FROM node WHERE uid = 'cdao:has_State') AND " +
        "is_a_link.predicate_id = (SELECT node_id FROM node WHERE uid = 'OBO_REL:is_a') AND " +
        "exhibits_link.predicate_id = (SELECT node_id FROM node WHERE uid = 'PHENOSCAPE:exhibits') AND " +
        "exhibits_link.is_inferred = 'f' AND " +
        "supertaxon_node.uid = ?";

    private final String queryForCountOfDatasetsUnderClade = 
        "SELECT DISTINCT " +
        "ds_node.uid AS character " +
        "FROM " +
        "node AS ds_node " +
        "JOIN (link AS has_tu_link " +
        "JOIN (node AS tu_node " +
        "JOIN (link AS has_taxon_link " +
        "JOIN (node AS taxon_node " +
        "JOIN (link AS is_a_link " +
        "JOIN node AS supertaxon_node " +
        "ON (supertaxon_node.node_id = is_a_link.object_id AND " +
        "	is_a_link.predicate_id = (SELECT node_id FROM node WHERE uid = 'OBO_REL:is_a'))) " +
        "ON (is_a_link.node_id = taxon_node.node_id)) " +
        "ON (has_taxon_link.object_id = taxon_node.node_id AND " +
        "	has_taxon_link.predicate_id = (SELECT node_id FROM node WHERE uid = 'PHENOSCAPE:has_taxon'))) " +
        "ON (has_taxon_link.node_id = tu_node.node_id)) " +
        "ON (has_tu_link.object_id = tu_node.node_id AND " +
        "	has_tu_link.predicate_id = (SELECT node_id FROM node WHERE uid = 'cdao:has_TU'))) " +
        "ON (has_tu_link.node_id = ds_node.node_id) " +
        "WHERE supertaxon_node.uid = ?";

    private final String queryForCountOfTaxaUnderClade = 
        "SELECT DISTINCT " +
        "taxon_node.uid AS taxon_uid, taxon_node.label AS taxon " +
        "FROM " +
        "node AS ds_node " +
        "JOIN (link AS has_tu_link " +
        "JOIN (node AS tu_node " +
        "JOIN (link AS has_taxon_link " +
        "JOIN (node AS taxon_node " +
        "JOIN (link AS is_a_link " +
        "JOIN node AS supertaxon_node " +
        "ON (supertaxon_node.node_id = is_a_link.object_id AND " +
        "	is_a_link.predicate_id = (SELECT node_id FROM node WHERE uid = 'OBO_REL:is_a'))) " +
        "ON (is_a_link.node_id = taxon_node.node_id)) " +
        "ON (has_taxon_link.object_id = taxon_node.node_id AND " +
        "	has_taxon_link.predicate_id = (SELECT node_id FROM node WHERE uid = 'PHENOSCAPE:has_taxon'))) " +
        "ON (has_taxon_link.node_id = tu_node.node_id)) " +
        "ON (has_tu_link.object_id = tu_node.node_id AND " +
        "	has_tu_link.predicate_id = (SELECT node_id FROM node WHERE uid = 'cdao:has_TU'))) " +
        "ON (has_tu_link.node_id = ds_node.node_id) " +
        "WHERE supertaxon_node.uid = ?";

    @Get("json")
    public Representation getJSONRepresentation() throws ResourceException {
        try {
            connectShardToDatabase();
            conn = this.getShard().getConnection();
            jObj = new JSONObject();
            listOfCladeObjs = new ArrayList<JSONObject>();

            this.findCountsOfCharactersDatasetsAndTaxaByClade();

            return new JsonRepresentation(jObj);
        } catch (SQLException sqle) {
            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, "[SQL EXCEPTION] Something broke server side. Consult server logs");
        } catch (ClassNotFoundException e) {
            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, "[CLASS NOT FOUND EXCEPTION] Something broke server side. Consult server logs");
        } catch (JSONException jsone) {
            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, "[JSON EXCEPTION] Something broke while assembling the JSON");
        } finally {
            disconnectShardFromDatabase();
        }
        return null;
    }

    private void findCountsOfCharactersDatasetsAndTaxaByClade() throws SQLException, JSONException{
        int countOfCharactersForClade = 0, countOfDatasetsForClade = 0, countOfTaxaForClade = 0;
        JSONObject cladeObj;
        for(String cladeID : ORDERS){
            Node cladeNode = this.getShard().getNode(cladeID);
            countOfCharactersForClade = findCountOfCharactersInClade(cladeID);
            countOfDatasetsForClade = findCountOfDatasetsInClade(cladeID);
            countOfTaxaForClade = findCountOfTaxaInClade(cladeID);
            cladeObj = new JSONObject();
            cladeObj.put("id", cladeID);
            cladeObj.put("name", cladeNode.getLabel());
            cladeObj.put("countOfTaxa", countOfTaxaForClade);
            cladeObj.put("countOfDatasets", countOfDatasetsForClade);
            cladeObj.put("countOfCharacters", countOfCharactersForClade);
            listOfCladeObjs.add(cladeObj);
        }
        jObj.put("countsOfCharactersTaxaAndDatasetsByClade", listOfCladeObjs);
    }

    private int findCountOfCharactersInClade(String cladeID) throws SQLException{
        int count = 0;
        PreparedStatement ps = conn.prepareStatement(queryForCountOfCharactersUnderClade);
        ps.setString(1, cladeID);
        ResultSet rs = ps.executeQuery();
        while(rs.next())
            ++count;
        return count;
    }

    private int findCountOfDatasetsInClade(String cladeID) throws SQLException{
        int dataSetCtForClade = 0;
        PreparedStatement pStmt = conn.prepareStatement(this.queryForCountOfDatasetsUnderClade);
        pStmt.setString(1, cladeID);
        ResultSet rs = pStmt.executeQuery();
        while(rs.next())
            ++dataSetCtForClade; 
        return dataSetCtForClade;
    }

    private int findCountOfTaxaInClade(String cladeID) throws SQLException{
        int taxaCtForClade = 0;
        PreparedStatement pStmt = conn.prepareStatement(this.queryForCountOfTaxaUnderClade);
        pStmt.setString(1, cladeID);
        ResultSet rs = pStmt.executeQuery();
        while(rs.next())
            ++taxaCtForClade; 
        return taxaCtForClade;
    }
}
