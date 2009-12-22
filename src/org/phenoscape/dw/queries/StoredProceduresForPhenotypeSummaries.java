package org.phenoscape.dw.queries;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.obd.query.impl.OBDSQLShard;
import org.obd.ws.util.dto.PhenotypeDTO;

public class StoredProceduresForPhenotypeSummaries {

	public static final String invokeStoredProcedureForTaxonSummary = "SELECT * FROM get_phenotype_summary_for_taxon(?)";
	public static final String invokeStoredProcedureForGeneSummary = "SELECT * FROM get_phenotype_summary_for_gene(?)";
	
	private OBDSQLShard shard;
	private Connection conn; 
	
	public StoredProceduresForPhenotypeSummaries(OBDSQLShard shard){
		this.shard = shard;
		this.conn = shard.getConnection();
	}
	
	public Collection<PhenotypeDTO> executeStoredProcedureAndAssembleResults(String storedProc, String parameter) throws SQLException{
		
		/** <a> annotationToReifsMap </a> 
		 * This new map has been added tp consolidate reif ids for a TAXON to PHENOTYPE assertion*/
		Map<PhenotypeDTO, Set<String>> annotationToReifsMap = new HashMap<PhenotypeDTO, Set<String>>();
		Collection<PhenotypeDTO> results = new ArrayList<PhenotypeDTO>();
		
		CallableStatement cs = conn.prepareCall(storedProc);
		cs.setString(1, parameter);
		ResultSet rs = cs.executeQuery();
		
		PhenotypeDTO pdto;
		
		while(rs.next()){
			pdto = new PhenotypeDTO(rs.getString(1));
			pdto.setTaxonId(rs.getString(2));
			pdto.setTaxon(rs.getString(3));
			pdto.setQualityId(rs.getString(4));
			pdto.setQuality(rs.getString(5));
			pdto.setCharacterId(rs.getString(6));			
			pdto.setCharacter(rs.getString(7));
			pdto.setEntityId(rs.getString(8));
			pdto.setEntity(rs.getString(9));
			pdto.setNumericalCount(rs.getString(11));
			String reif = rs.getString(10);
			
			pdto.setRelatedEntityId(rs.getString(12));
			pdto.setRelatedEntity(rs.getString(13));
			pdto.setPublication(rs.getString(14));
			
			//we collect all the reif ids associated with each DTO object
			Set<String> reifs = annotationToReifsMap.get(pdto);
			if(reifs == null)
				reifs = new HashSet<String>();
			reifs.add(reif);
			annotationToReifsMap.put(pdto, reifs);
		}
		
		for(PhenotypeDTO dto : annotationToReifsMap.keySet()){
			//here we reassign the reif ids to the DTO
			String reifString = "";
			for(String reif : annotationToReifsMap.get(dto)){
				reifString += reif + ",";
			}
			//we trim out the last comma
			reifString = reifString.substring(0, reifString.lastIndexOf(","));
			dto.setReifId(reifString);
			results.add(dto);
		}
		return results;
	}
}
