package org.obd.ws.statistics.reports.resources;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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

public class DataConsistencyReportGeneratorForQuestion21A extends AbstractStatisticalReportGenerator{
	
	public static final String 
		QUERY_FOR_PHENOTYPES_WITH_RELATIONAL_QUALITIES_AND_LACKING_RELATED_ENTITIES = 
			"SELECT DISTINCT " +
			"_character.quality_label AS attribute, " +
			"entity1.entity_label AS entity, " +
			"quality.quality_label AS quality,  " +
			"pub_dw.authors AS authors, " +
			"pub_dw.publication_year AS pub_year, " +
			"_number.val AS character_number, " +
			"character_node.label AS character_label " +
			"FROM " +
			"dw_phenotype_table AS phenotype " +
			"JOIN dw_entity_table AS entity1 " +
			"ON (entity1.entity_nid = phenotype.inheres_in_entity_nid) " +
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
			"phenotype.character_nid IN " +
			"(SELECT node_id FROM node WHERE uid IN (?,?,?)) AND " + 
			"phenotype.towards_entity_nid IS NULL";
	
	public final String TITLE_LINE = "ATTRIBUTE\tENTITY\tQUALITY\tPUBLICATION\tCHARACTER NUMBER\tLABEL\n";
	
	private Connection conn;
	private OBDSQLShard shard; 
	
	public DataConsistencyReportGeneratorForQuestion21A(Context context, Request request,
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
			 String lines = this.getPhenotypesWithRelationalQualitiesLackingRelatedEntities();
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
	 
	 private String getPhenotypesWithRelationalQualitiesLackingRelatedEntities() {
		int ct = 0;
		String line = "", attribute, entity, quality, authors, year, number, character, publication;

		try {
			
			PreparedStatement ps = conn
					.prepareStatement(QUERY_FOR_PHENOTYPES_WITH_RELATIONAL_QUALITIES_AND_LACKING_RELATED_ENTITIES);
			ps.setString(1, this.RELATIONAL_SHAPE_QUALITY_IDENTIFIER);
			ps.setString(2, this.RELATIONAL_SPATIAL_QUALITY_IDENTIFIER);
			ps.setString(3, this.RELATIONAL_STRUCTURAL_QUALITY_IDENTIFIER);
			
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				++ct;
				attribute = rs.getString(1);
				entity = rs.getString(2);
				quality = rs.getString(3);
				authors = rs.getString(4);
				year = rs.getString(5);
				number = rs.getString(6);
				character = rs.getString(7);
				publication = authors + ", " + year;
				line += attribute + "\t" + entity + "\t" + quality + "\t"
						+ publication + "\t" + number + "\t" + character + "\n";
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
