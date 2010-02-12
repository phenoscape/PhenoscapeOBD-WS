package org.obd.ws.statistics.resources;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;
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

public class CharactersAndGenesByAttribute extends AbstractStatisticsGenerator{

	private Connection conn;
	private OBDSQLShard shard; 
	
	private JSONObject jObj;
	private List<JSONObject> listOfAttributeObjs;
	
	private final String queryForCharactersByRelationalAttribute = 
		"SELECT DISTINCT " +
		"character_node.label AS _character " +
		"FROM " +
		"node AS character_node " +
		"JOIN (link AS has_state_link " +
		"JOIN (node AS state_node " +
		"JOIN (link AS has_phenotype_link " +
		"JOIN (node AS phenotype_node " +
		"JOIN dw_phenotype_table AS phenotype " +
		"ON (phenotype.phenotype_nid = phenotype_node.node_id)) " +
		"ON (phenotype_node.node_id = has_phenotype_link.object_id AND " +
		"	has_phenotype_link.predicate_id = (SELECT node_id FROM node WHERE uid = 'cdao:has_Phenotype'))) " +
		"ON (has_phenotype_link.node_id	= state_node.node_id)) " +
		"ON (state_node.node_id = has_state_link.object_id AND " +
		"	has_state_link.predicate_id = (SELECT node_id FROM node WHERE uid = 'cdao:has_Datum'))) " +
		"ON (has_state_link.node_id = character_node.node_id) " +
		"WHERE " +
		"phenotype.character_nid = (SELECT node_id FROM node WHERE uid = ?)";
	
	private final String queryForGenesByRelationalAttribute = 
		"SELECT DISTINCT " +
		"ggp.gene_nid AS gene " +
		"FROM " +
		"dw_gene_genotype_phenotype_table AS ggp " +
		"JOIN dw_phenotype_table AS phenotype " +
		"ON (phenotype.phenotype_nid = ggp.phenotype_nid) " +
		"WHERE " +
		"phenotype.character_nid = (SELECT node_id FROM node WHERE uid = ?)";

	public CharactersAndGenesByAttribute(Context context, Request request,
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
			 listOfAttributeObjs = new ArrayList<JSONObject>();
			 
			 this.getCountsOfGenesAndCharactersForEachAttribute();
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
	
	 
	 /**
		 * These two methods return characters by relational attribute
	 * @throws JSONException 
		 */
		private void getCountsOfGenesAndCharactersForEachAttribute() throws SQLException, JSONException{
			JSONObject attributeObj;
			int countOfCharacters = 0, countOfGenes = 0;
			for (String attributeID : ATTRIBUTES) {
				String attributeName = this.shard.getNode(attributeID).getLabel();
				countOfCharacters = findCountOfCharactersForAttribute(attributeID);
				countOfGenes = this.findCountOfGenesForAttribute(attributeID);
				attributeObj = new JSONObject();
				attributeObj.put("id", attributeID);
				attributeObj.put("name", attributeName);
				attributeObj.put("countOfCharacters", countOfCharacters);
				attributeObj.put("countOfGenes", countOfGenes);
				listOfAttributeObjs.add(attributeObj);
			}
			jObj.put("countsOfCharactersAndGenesByAttribute", listOfAttributeObjs);
		}

		private int findCountOfCharactersForAttribute(String attributeID) throws SQLException{
			int ct = 0;
			PreparedStatement ps = conn.prepareStatement(queryForCharactersByRelationalAttribute);
			ps.setString(1, attributeID);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) 
					ct++;
			return ct;
		}
		
		private int findCountOfGenesForAttribute(String attributeID) throws SQLException{
			int ct = 0;
			PreparedStatement ps = conn.prepareStatement(queryForGenesByRelationalAttribute);
			ps.setString(1, attributeID);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) 
					ct++;
			return ct;
		}
}
