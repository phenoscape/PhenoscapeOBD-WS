package org.phenoscape.obd;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.obd.model.Node;
import org.obd.model.Statement;
import org.obd.query.Shard;
import org.obd.query.impl.AbstractSQLShard;

/**
 * This class interfaces with methods from the OBDAPI, specifically
 * in the Shard class hierarchy, and invokes these methods. Methods
 * defined in this class return their results to invoking REST 
 * resources
 */

public class OBDQuery {

	private final Shard shard;
	private Connection conn;
	public Logger log;
	public static enum AutoCompletionMatchTypes{
		LABEL_MATCH, SYNONYM_MATCH, DEFINITION_MATCH
	};
	
	
	/**
	 * This is the default constructor. 
	 * @param shard = the shard to use
	 */
	public OBDQuery(Shard shard){
		this.shard = shard;
		this.conn = ((AbstractSQLShard)shard).getConnection();
		this.log = Logger.getLogger(this.getClass());
	}

	/**
	 * @author cartik
	 * @param queryString
	 * @param searchTerm
	 * @return
	 * @throws SQLException
	 * @PURPOSE The purpose of this query is to retrieve all the homolog information from
	 * the database and package them into a collection of nodes. All the retrieved information
	 * is added to these Nodes via links
	 */
	public Collection<Node> executeHomologyQueryAndAssembleResults(String queryString, String searchTerm) 
		throws SQLException{
		Collection<Node> results = new ArrayList<Node>();
		PreparedStatement pStmt = null;
		
		try{
			pStmt = conn.prepareStatement(queryString);
			for(int i = 1; i <= pStmt.getParameterMetaData().getParameterCount(); i++)
				pStmt.setString(i, searchTerm);
			log.trace(pStmt.toString());
			long startTime = System.currentTimeMillis();
			ResultSet rs = pStmt.executeQuery();
			long endTime = System.currentTimeMillis();
			log.trace("Query execution took  " + (endTime -startTime) + " milliseconds");
			while(rs.next()){
				Node homologNode = new Node(rs.getString(1) + "\thasHomolog\t" + rs.getString(6));
				Statement lhTaxonIdStmt = new Statement(homologNode.getId(), "lhTaxonId", rs.getString(2));
				Statement lhTaxonStmt = new Statement(homologNode.getId(), "lhTaxon", rs.getString(3));
				Statement rhTaxonIdStmt = new Statement(homologNode.getId(), "rhTaxonId", rs.getString(7));
				Statement rhTaxonStmt = new Statement(homologNode.getId(), "rhTaxon", rs.getString(8));
				Statement lhEntityIdStmt = new Statement(homologNode.getId(), "lhEntityId", rs.getString(4));
				Statement lhEntityStmt = new Statement(homologNode.getId(), "lhEntity", rs.getString(5));
				Statement rhEntityIdStmt = new Statement(homologNode.getId(), "rhEntityId", rs.getString(9));
				Statement rhEntityStmt = new Statement(homologNode.getId(), "rhEntity", rs.getString(10));
				
				String publication = rs.getString(11);
				String evidenceCode = rs.getString(12);
				String evidence = rs.getString(13);
				
				Statement evidenceCodeStmt = new Statement(homologNode.getId(), "hasEvidenceCode", evidenceCode);
				Statement evidenceStmt = new Statement(homologNode.getId(), "hasEvidence", evidence);
				Statement sourceStmt = new Statement(homologNode.getId(), "hasPublication", publication); 
				
				homologNode.addStatement(lhEntityIdStmt);
				homologNode.addStatement(lhEntityStmt);
				homologNode.addStatement(lhTaxonIdStmt);
				homologNode.addStatement(lhTaxonStmt);
				homologNode.addStatement(rhEntityIdStmt);
				homologNode.addStatement(rhEntityStmt);
				homologNode.addStatement(rhTaxonIdStmt);
				homologNode.addStatement(rhTaxonStmt);
				homologNode.addStatement(sourceStmt);
				homologNode.addStatement(evidenceCodeStmt);
				homologNode.addStatement(evidenceStmt);
				
				results.add(homologNode);
			}
		}
		catch(SQLException sqle){
			log.error(sqle);
			throw sqle;
		}
		finally {
			if (pStmt != null) {
				try { pStmt.close(); }
				catch (SQLException ex) {
					log.error(ex);
                    // let's not worry further about the close() failing
					throw ex;
				}
			}
		}
		return results;
	}
	
	/**@throws SQLException 
	 * @PURPOSE The purpose of this method is to execute the given query with set search term and assemble the results into a 
	 * makeshift persistence layer. The persistence layer is a collection of Nodes from the OBD model. Columns from each retrieved
	 * row are packaged into a single Node object. The phenotype is the central entity in this querying procedure. 
	 * Therefore, the IDs of the each node are set to be the retrieved phenotype. The method assembles
	 * these nodes into a collection, which are returned
	 */

	public Collection<Node> executeQueryAndAssembleResults(String queryStr, String searchTerm, Map<String, String> filterOptions) 
		throws SQLException{
		Collection<Node> results = new ArrayList<Node>();
		String entityLabel;
		PreparedStatement pstmt = null;
		Map<String, String> filterableValues; 
		try{
			pstmt = conn.prepareStatement(queryStr);
			for(int i = 1; i <= pstmt.getParameterMetaData().getParameterCount(); i++)
				pstmt.setString(i, searchTerm);
			log.trace(pstmt.toString());
			long startTime = System.currentTimeMillis();
			ResultSet rs = pstmt.executeQuery();
			long endTime = System.currentTimeMillis();
			log.trace("Query execution took  " + (endTime -startTime) + " milliseconds");
			while(rs.next()){
                 filterableValues = new HashMap<String, String>();
                 filterableValues.put("subject", rs.getString(2));
                 filterableValues.put("entity", rs.getString(8));
                 filterableValues.put("character", rs.getString(6));
				/*
				 * each retrieved row is filtered by passing the filterable values as arguments to a generic 
				 * filtering method. if the filtering method returns a boolean true for a row, this row is
				 * excluded from the results. if it returns false, then the row is packaged into a Node object
				 * and added to the collection which is returned by this method
				 */
				
				if(!isFilterableRow(filterOptions, filterableValues)){
					if(!rs.getString(2).contains("GENO")){ //sometimes, genotypes are returned and we don;t want these
						Node phenotypeNode = new Node(rs.getString(1));
						/*
						 * We keep track of both labels and uids, so we dont have to go looking for labels for uids
						 * at the REST resource level. The queries return everything anyways.
						 */
						Statement taxonOrGeneSt = new Statement(phenotypeNode.getId(), "exhibitedBy", rs.getString(3));
						Statement taxonOrGeneIdSt = new Statement(phenotypeNode.getId(), "exhibitedById", rs.getString(2));
						Statement stateSt = new Statement(phenotypeNode.getId(), "hasState", rs.getString(5));
						Statement stateIdSt = new Statement(phenotypeNode.getId(), "hasStateId", rs.getString(4));
						Statement characterSt = new Statement(phenotypeNode.getId(), "hasCharacter", rs.getString(7));
						Statement characterIdSt = new Statement(phenotypeNode.getId(), "hasCharacterId", rs.getString(6));
						entityLabel = rs.getString(9);
						if(entityLabel == null){
							entityLabel = resolveLabel(rs.getString(8));
						}	
						Statement entitySt = new Statement(phenotypeNode.getId(), "inheresIn", entityLabel);
						Statement entityIdSt = new Statement(phenotypeNode.getId(), "inheresInId", rs.getString(8));
						Statement reifIdSt =  new Statement(phenotypeNode.getId(), "hasReifId", rs.getString(10));
						phenotypeNode.addStatement(taxonOrGeneSt);
						phenotypeNode.addStatement(taxonOrGeneIdSt);
						phenotypeNode.addStatement(stateSt);
						phenotypeNode.addStatement(stateIdSt);
						phenotypeNode.addStatement(characterSt);
						phenotypeNode.addStatement(characterIdSt);
						phenotypeNode.addStatement(entitySt);
						phenotypeNode.addStatement(entityIdSt);
						phenotypeNode.addStatement(reifIdSt);
						results.add(phenotypeNode);
					}
				}
			}
		}
		catch(SQLException sqle){
			log.error(sqle);
			throw sqle;
		}
		finally {
			if (pstmt != null) {
                            try { pstmt.close(); }
                            catch (SQLException ex) {
                		log.error(ex);
                		// let's not worry further about the close() failing
                            }
                        }
		}
		return results;
	}
	
	/**
	 * @param filterOptions. These are set by the calling method, they come directly from user input
	 * @param filterableValues. These are the ids present in the various columns of the row that is being checked
	 * 
	 * The purpose of this method is to check a row against the filter values. If the row contains ALL the filter values, 
	 * ths method returns a false meaning the row is NOT to be filtered.
	 */
    
	private boolean isFilterableRow(Map<String, String> filterOptions, Map<String, String> filterableValues) {
		for(String key : filterOptions.keySet()){
			String filterValue = filterOptions.get(key);
			String valueFromQueryResult = filterableValues.get(key);
			if(filterValue != null && valueFromQueryResult != null && !filterValue.equals(valueFromQueryResult)){
					return true;
			}
		}
		return false;
	}

	 /**
     * @PURPOSE: This method takes a post composition and creates a 
     * legible label for it. Because the label field in the DB is null
     * for post composed terms
     * @PROCEDURE: This method substitutes all UIDs with approp. labels eg. 
     * TAO:0001173 with 'Dorsal fin'. Carats (^) and underscores are replaced 
     * with white spaces.
     * @param cd - CompositionalDescription. is of the form (<entity_uid>^ <rel_uid>(<entity_uid>)
     * @return label - is of the form (<entity_label> <rel_label> <entity_label>
     */
	
	public String resolveLabel(String cd){
		String label = cd.replaceAll("\\^", " ");
		String oldLabel = label;
		/* A PATTERN FOR UIDS */
		Pattern pat = Pattern.compile("[A-Z]+_?[A-Z]*:[0-9a-zA-Z]+_?[0-9a-zA-Z]*");
		Matcher m = pat.matcher(oldLabel);
		while(m.find()){
			String s2replace = oldLabel.substring(m.start(), m.end());
			String replaceS = shard.getNode(s2replace).getLabel();
			if(replaceS == null)
				replaceS = s2replace.substring(s2replace.indexOf(":") + 1);
			label = label.replace(s2replace, replaceS);
		}
		label = label.replace("_", " ");
		return label;
	}
	
	/**
	 * @PURPOSE: This method looks for information pertaining to a specific term
	 * including parent and child nodes of the term
	 * @author cartik
	 * @param term
	 * @return
	 * THis method has been developed for generic term searches
	 */
	
	public Set<Statement> genericTermSearch(String term, String source){
		if(term == null){
			throw new IllegalArgumentException("ERROR: Input parameter is null");
		}
		
		Set<Statement> results = new HashSet<Statement>();
                // search term first as subject, then as object of triples 
		results.addAll(this.shard.getStatementsWithSearchTerm(term, null, null, source, false, false));
		results.addAll(this.shard.getStatementsWithSearchTerm(null, null, term, source, false, false));
		return results;
	}
	
	/**
     * @PURPOSE The purpose of this method is to return matching nodes for
     * autocompletion
	 * @author cartik
	 * @param term - search term
	 * @param ontologyList - list of ontologies to filter terms from
	 * @param zfinOption - set separately because GENEs are not read from ontologies and
	 * must be queried separately
	 * @param synOption - synonym option
	 * @param defOption - definition option
	 * @return
	 * @throws SQLException 
	 */
	public Map<String, Collection<Node>> getCompletionsForSearchTerm(String term, boolean zfinOption, 
				List<String> ontologyList, boolean synOption, boolean defOption) throws SQLException{
		
		Map<String, Collection<Node>> results = new HashMap<String, Collection<Node>>();
		try{
			Collection<Node> nodesByName = this.shard.getNodesForSearchTermByLabel(term, zfinOption, ontologyList);

			results.put(AutoCompletionMatchTypes.LABEL_MATCH.name(), nodesByName);
			if(synOption){
				Collection<Node> nodesBySynonym = this.shard.getNodesForSearchTermBySynonym(term, zfinOption, ontologyList, true);
				results.put(AutoCompletionMatchTypes.SYNONYM_MATCH.name(), nodesBySynonym);
			}
			if(defOption){
				Collection<Node> nodesByDefinition = this.shard.getNodesForSearchTermByDefinition(term, zfinOption, ontologyList);
				results.put(AutoCompletionMatchTypes.DEFINITION_MATCH.name(), nodesByDefinition);
			}
		}
		catch(SQLException e){
                    log.error(e);
                    throw e;
		}
		return results;
	}	
	
}
