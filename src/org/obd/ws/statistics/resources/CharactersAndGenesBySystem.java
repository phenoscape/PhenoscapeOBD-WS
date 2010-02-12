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
import org.obd.query.impl.OBDSQLShard;
import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.resource.Representation;
import org.restlet.resource.ResourceException;
import org.restlet.resource.Variant;

public class CharactersAndGenesBySystem extends AbstractStatisticsGenerator{

	private Connection conn;
	private OBDSQLShard shard; 
	
	private JSONObject jObj;
	private List<JSONObject> listOfSystemObjs;
	
	private final String queryForCharactersBySystem = 
		"SELECT DISTINCT " +
		"character_node.node_id AS _character " +
		"FROM " +
		"node AS character_node " +
		"JOIN (link AS has_state_link " +
		"JOIN (node AS state_node " +
		"JOIN (link AS has_phenotype_link " +
		"JOIN (node AS phenotype_node " +
		"JOIN phenotype_inheres_in_part_of_entity AS _system " +
		"ON (_system.phenotype_nid = phenotype_node.node_id)) " +
		"ON (phenotype_node.node_id = has_phenotype_link.object_id AND " +
		"	has_phenotype_link.predicate_id = (SELECT node_id FROM node WHERE uid = 'cdao:has_Phenotype'))) " +
		"ON (has_phenotype_link.node_id	= state_node.node_id)) " +
		"ON (state_node.node_id = has_state_link.object_id AND " +
		"	has_state_link.predicate_id = (SELECT node_id FROM node WHERE uid = 'cdao:has_Datum'))) " +
		"ON (has_state_link.node_id = character_node.node_id) " +
		"WHERE _system.entity_uid = ?";
	
	private final String queryForGenesBySystem = 
		"SELECT DISTINCT " +
		"gene.gene_uid AS gene " +
		"FROM " +
		"dw_gene_table AS gene " +
		"JOIN (dw_gene_genotype_phenotype_table AS ggp " +
		"JOIN phenotype_inheres_in_part_of_entity AS pinpoe " +
		"ON (pinpoe.phenotype_nid = ggp.phenotype_nid)) " +
		"ON (ggp.gene_nid = gene.gene_nid) " +
		"WHERE " +
		"pinpoe.entity_uid = ?";
	
	public CharactersAndGenesBySystem(Context context, Request request,
			Response response) {
		super(context, request, response);
		this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
	}

	 @Override
	 public Representation represent(Variant variant) throws ResourceException {
		 try {
			 connectShardToDatabase();
			 shard = this.getShard();
			 conn = shard.getConnection();
			 jObj = new JSONObject();
			 listOfSystemObjs = new ArrayList<JSONObject>();
			 
			 this.findCountsOfGenesAndCharactersBySystem();
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
	 
	 /** These three methods return the count of phenotyped characters and genes per system
	  * @throws JSONException 
	 */
	private void findCountsOfGenesAndCharactersBySystem() throws JSONException {
		int countOfCharactersForSystem = 0, countOfGenesForSystem = 0;
		JSONObject systemObj;
		for (String systemID : SYSTEMS) {
			final Node systemNode = this.shard.getNode(systemID);
			countOfCharactersForSystem = printCountOfCharactersInheringInPartOfNode(systemID);
			countOfGenesForSystem = printCountOfGenesExpressedInSystem(systemID);
				
			systemObj = new JSONObject();
			systemObj.put("id", systemID);
			systemObj.put("name", systemNode.getLabel());
			systemObj.put("countOfCharacters", countOfCharactersForSystem);
			systemObj.put("countOfGenes", countOfGenesForSystem);
				
			listOfSystemObjs.add(systemObj);
		}
		jObj.put("countsOfGenesAndCharactersBySystem", listOfSystemObjs);
	}

	private int printCountOfCharactersInheringInPartOfNode(String nodeID) {
		int countOfCharacters = 0;
		try {
			PreparedStatement ps = conn.prepareStatement(queryForCharactersBySystem);
			ps.setString(1, nodeID);
			ResultSet rs = ps.executeQuery();
			while (rs.next())
				++countOfCharacters;
		} catch (SQLException sqle) {
			sqle.printStackTrace();
		}
		return countOfCharacters;
	}
		
	private int printCountOfGenesExpressedInSystem(String systemUID){
		int ct = 0;
	    try {
	    	PreparedStatement ps = conn.prepareStatement(queryForGenesBySystem);
			ps.setString(1, systemUID);
			ResultSet rs = ps.executeQuery();
			while(rs.next()){
				++ct;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return ct; 
    }
}
