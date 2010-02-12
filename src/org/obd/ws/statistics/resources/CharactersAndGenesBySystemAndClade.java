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

public class CharactersAndGenesBySystemAndClade extends AbstractStatisticsGenerator{

	private Connection conn;
	private OBDSQLShard shard; 
	
	private JSONObject jObj;
	private List<JSONObject> listOfCladeObjs;
	
	private final String queryForCharactersBySystemAndClade = 
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
    	"JOIN (dw_taxon_phenotype_table AS tp " +
    	"JOIN dw_taxon_is_a_taxon_table AS tisat " +
    	"ON (tisat.subtaxon_nid = tp.taxon_nid)) " +
    	"ON (tp.phenotype_nid = phenotype_node.node_id) " +
    	"WHERE " +
    	"_system.entity_uid = ? AND " +
    	"tisat.supertaxon_nid = (SELECT node_id FROM node WHERE uid = ?)";
	
	private final String queryForGenesBySystemAndClade = 
    	"SELECT DISTINCT " +
    	"ggp.gene_nid AS gene " +
    	"FROM " +
    	"dw_gene_genotype_phenotype_table AS ggp " +
    	"JOIN (node AS phenotype_node " +
    	"JOIN phenotype_inheres_in_part_of_entity AS _system " +
    	"ON (_system.phenotype_nid = phenotype_node.node_id)) " +
    	"ON (phenotype_node.node_id = ggp.phenotype_nid) " +
    	"JOIN (dw_taxon_phenotype_table AS tp " +
    	"JOIN dw_taxon_is_a_taxon_table AS tisat " +
    	"ON (tisat.subtaxon_nid = tp.taxon_nid)) " +
    	"ON (tp.phenotype_nid = phenotype_node.node_id) " +
    	"WHERE " +
    	"_system.entity_uid = ? AND " +
    	"tisat.supertaxon_nid = (SELECT node_id FROM node WHERE uid = ?)";
	
	public CharactersAndGenesBySystemAndClade(Context context, Request request,
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
			 listOfCladeObjs = new ArrayList<JSONObject>();

			 this.findCountsOfGenesAndCharactersBySystemAndClade();
			 
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
	 * These two methods return counts of characters by system and clade
	 * @throws JSONException 
	 */

	private void findCountsOfGenesAndCharactersBySystemAndClade() throws JSONException {
		JSONObject systemObj, cladeObj;
		Node taxonNode, systemNode;
		int countOfCharacters = 0, countOfGenes = 0;
		
		for (String taxonID : ORDERS) {
			taxonNode = this.shard.getNode(taxonID);
			cladeObj = new JSONObject();
			List<JSONObject> listOfSystemObjs = new ArrayList<JSONObject>();
			
			for (String systemID : SYSTEMS) {
				systemNode = this.shard.getNode(systemID);
				systemObj = new JSONObject();
				
				countOfCharacters = getCountOfCharactersAssociatedWithSystemAndClade(systemID, taxonID);
				countOfGenes = getCountOfGenesAssociatedWithSystemAndClade(systemID, taxonID);
				
				systemObj.put("id", systemID);
				systemObj.put("name", systemNode.getLabel());
				systemObj.put("countOfCharacters", countOfCharacters);
				systemObj.put("countOfGenes", countOfGenes);
				listOfSystemObjs.add(systemObj);
			}
			cladeObj.put("id", taxonID);
			cladeObj.put("name", taxonNode.getLabel());
			cladeObj.put("countsBySystem", listOfSystemObjs);
			listOfCladeObjs.add(cladeObj);
		}
		jObj.put("countsOfGenesAndCharactersBySystemAndClade", listOfCladeObjs);
	}

	private int getCountOfCharactersAssociatedWithSystemAndClade(String systemID, String cladeID) {
		int count = 0;
		try {
			PreparedStatement ps = conn.prepareStatement(queryForCharactersBySystemAndClade);
			ps.setString(1, systemID);
			ps.setString(2, cladeID);

			ResultSet rs = ps.executeQuery();
			while (rs.next())
				++count;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return count;
	}
	
	private int getCountOfGenesAssociatedWithSystemAndClade(String systemID, String cladeID){
		int count = 0;
		try {
			PreparedStatement ps = conn.prepareStatement(queryForGenesBySystemAndClade);
			ps.setString(1, systemID);
			ps.setString(2, cladeID);

			ResultSet rs = ps.executeQuery();
			while (rs.next())
				++count;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return count;
	}
}
