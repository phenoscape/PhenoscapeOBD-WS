package org.obd.ws.statistics.reports.resources;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.obd.query.impl.OBDSQLShard;
import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;
import org.restlet.resource.ResourceException;
import org.restlet.resource.StringRepresentation;
import org.restlet.resource.Variant;

public class DataConsistencyReportGeneratorForQuestion13 extends AbstractStatisticalReportGenerator{
	
	public final String 
		QUERY_FOR_CHARACTER_WITH_ONLY_ONE_OF_TWO_POSSIBLE_ANNOTATIONS = 
			"SELECT " +
    		"publication.authors AS authors, " +
    		"_number.val AS char_number, " +
    		"character_node.label AS char_label, " +
    		"tagval.val AS char_comment, " +
    		"publication.publication_year AS _year " +
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
    		"JOIN dw_publication_table AS publication " +
    		"ON (publication.publication = pub_node.uid)) " +
    		"ON (pub_node.node_id = has_publication_link.object_id AND " +
    		"	has_publication_link.predicate_id = (SELECT node_ID FROM node WHERE uid = 'PHENOSCAPE:has_publication'))) " +
    		"ON (has_publication_link.node_id = ds_link.node_id AND " +
    		"	ds_link.predicate_id = (SELECT node_id FROM node WHERE uid = 'cdao:has_Character'))) " +
    		"ON (ds_link.object_id = character_node.node_id) " +
    		"WHERE " +
    		"character_node.node_id = ?";
	
	public final String 
		QUERY_FOR_CHARACTERS_WITH_PRESENCE_AND_ABSENCE_ANNOTATIONS =
			"SELECT DISTINCT " +
    		"character_node.node_id AS _char, " +
    		"quality.quality_label AS quality " +
    		"FROM " +
    		"node AS character_node " +
    		"JOIN (link AS has_state_link " +
    		"JOIN (node AS state_node " +
    		"JOIN (link AS has_phenotype_link " +
    		"JOIN (node AS phenotype_node " +
    		"JOIN (dw_phenotype_table AS phenotype " +
    		"JOIN dw_quality_table AS quality " +
    		"ON (quality.quality_nid = phenotype.is_a_quality_nid)) " +
    		"ON (phenotype.phenotype_nid = phenotype_node.node_id)) " +
    		"ON (phenotype_node.node_id = has_phenotype_link.object_id AND " +
    		"	has_phenotype_link.predicate_id = (SELECT node_id FROM node WHERE uid = 'cdao:has_Phenotype'))) " +
    		"ON (has_phenotype_link.node_id	= state_node.node_id)) " +
    		"ON (state_node.node_id = has_state_link.object_id AND " +
    		"	has_state_link.predicate_id = (SELECT node_id FROM node WHERE uid = 'cdao:has_Datum'))) " +
    		"ON (has_state_link.node_id = character_node.node_id) " +
    		"WHERE " +
    		"phenotype.is_a_quality_nid IN (SELECT node_id FROM node WHERE uid IN ('PATO:0000467', 'PATO:0000462'))";
	
	public final String TITLE_LINE = "LABEL\tPUBLICATION\tCHARACTER NUMBER\tCOMMENT\tSTATE\n";
	
	private Connection conn;
	private OBDSQLShard shard; 

	private String lines;

	private Map<Integer, Set<String>> mapFromCharactersToPresenceAndAbsenceAnnotations;
	
	public DataConsistencyReportGeneratorForQuestion13(Context context, Request request,
			Response response) {
		super(context, request, response);
		this.getVariants().add(new Variant(MediaType.TEXT_PLAIN));
		
	}

	 @Override
	 public Representation represent(Variant variant) throws ResourceException {
		 try {
			 connectShardToDatabase();
			 shard = this.getShard();
			 conn = shard.getConnection();
			 mapFromCharactersToPresenceAndAbsenceAnnotations = new HashMap<Integer, Set<String>>();
			 
			 lines = this.printCharactersWithOnlyOneOfTwoPossibleAnnotations();
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

	 public String printCharactersWithOnlyOneOfTwoPossibleAnnotations(){
	    	int ct = 0;
	    	Set<String> presenceAbsenceSet;
	    	String line = "";
	    	
	    	this.populateMapFromCharactersToPresenceAndAbsenceAnnotations();
	    	
	    	for(Integer character : this.mapFromCharactersToPresenceAndAbsenceAnnotations.keySet()){
	    		presenceAbsenceSet = this.mapFromCharactersToPresenceAndAbsenceAnnotations.get(character);
	    		if(presenceAbsenceSet.size() < 2){
	    			++ct;
	    			line += this.printCharactersWithOnlyOneOfTwoPossibleAnnotations(character, presenceAbsenceSet) + "\n";
	    		}
	    	}
	    	if(ct > 0){
    			line = TITLE_LINE + line;
	    	}
	    	return line; 
	    }
	    
	    private String printCharactersWithOnlyOneOfTwoPossibleAnnotations(Integer characterNodeID, Set<String> presenceAbsenceSet){
	    	String authors, number, label, comment, _year, publication, line = ""; 
	    	
	    	try {
				PreparedStatement ps = 
					conn.prepareStatement(QUERY_FOR_CHARACTER_WITH_ONLY_ONE_OF_TWO_POSSIBLE_ANNOTATIONS);
				ps.setInt(1, characterNodeID);
				ResultSet rs = ps.executeQuery();
				while(rs.next()){
					authors = rs.getString(1);
					number = rs.getString(2);
					label = rs.getString(3);
					comment = rs.getString(4);
					if(comment == null || comment.trim().length() == 0)
						comment = "";
					_year = rs.getString(5);
					publication = authors + ", " + _year;
					line = label + "\t" + publication + "\t" + number + 
							"\t" + comment + "\t" + presenceAbsenceSet;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			return line; 
	    }
	    
	    private void populateMapFromCharactersToPresenceAndAbsenceAnnotations(){
	    	
	    	String quality;
	    	Integer character;
	    	Set<String> presenceAbsenceSet;
	    	
	    	try {
				PreparedStatement ps = 
					conn.prepareStatement(QUERY_FOR_CHARACTERS_WITH_PRESENCE_AND_ABSENCE_ANNOTATIONS);
				ResultSet rs = ps.executeQuery();
				while(rs.next()){
					character = rs.getInt(1);
					quality = rs.getString(2);
					if(this.mapFromCharactersToPresenceAndAbsenceAnnotations.keySet().contains(character))
						presenceAbsenceSet = this.mapFromCharactersToPresenceAndAbsenceAnnotations.get(character);
					else
						presenceAbsenceSet = new HashSet<String>();
					presenceAbsenceSet.add(quality);
					this.mapFromCharactersToPresenceAndAbsenceAnnotations.put(character, presenceAbsenceSet);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
	    }
}
