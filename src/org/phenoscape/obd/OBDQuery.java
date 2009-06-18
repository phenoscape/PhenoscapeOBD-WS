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
import org.obd.model.CompositionalDescription;
import org.obd.model.Node;
import org.obd.model.Statement;
import org.obd.query.Shard;
import org.obd.query.impl.AbstractSQLShard;
import org.obd.ws.util.Collections;
import org.obd.ws.util.Queries;
import org.obd.ws.util.dto.AnnotationDTO;
import org.obd.ws.util.dto.HomologDTO;
import org.obd.ws.util.dto.PhenotypeDTO;

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
	private Queries queries;
	public static enum AutoCompletionMatchTypes{
		LABEL_MATCH, SYNONYM_MATCH, DEFINITION_MATCH
	};
	
	
	/**
	 * This is the default constructor. 
	 * @param shard = the shard to use. THis has to be an AbstractSQLShard to use the connection
	 * it comes with
	 */
	public OBDQuery(Shard shard){
		this.shard = shard;
		this.conn = ((AbstractSQLShard)shard).getConnection();
		this.log = Logger.getLogger(this.getClass());
		this.queries = new Queries(this.shard);
	}

	/**
	 * @author cartik
	 * @param reifLinkId - the search parameter for the SQL query. This is an INTEGER
	 * which is provided by the invoking REST service. 
	 * @return
	 * @throws SQLException
	 * @PURPOSE The purpose of this query is to return all the free text data
	 * (metadata) associated with a given <TAXON><EXHIBITS><PHENOTYPE> assertion
	 * The metadata is packaged into a collection of Nodes which are then returned
	 * back
	 */
	public Collection<AnnotationDTO> executeFreeTextQueryAndAssembleResults(Integer reifLinkId)
		throws SQLException{
		
		Collection<AnnotationDTO> results = new ArrayList<AnnotationDTO>();
		PreparedStatement pStmt = null;
		
		String entityId, entity, qualityId, quality;
		
		try{
			pStmt = conn.prepareStatement(queries.getFreeTextDataQuery());
			for(int i = 1; i <= pStmt.getParameterMetaData().getParameterCount(); i++)
				pStmt.setInt(i, reifLinkId);
			log.trace(pStmt.toString());
			long startTime = System.currentTimeMillis();
			ResultSet rs = pStmt.executeQuery();
			long endTime = System.currentTimeMillis();
			log.trace("Query execution took  " + (endTime -startTime) + " milliseconds");
			if(rs.next()){
				AnnotationDTO dto = new AnnotationDTO(rs.getString(1));
				
				dto.setTaxonId(rs.getString(2));
				dto.setTaxon(rs.getString(3));
				
				entityId = rs.getString(4);
				entity = rs.getString(5);
				
				//Handling post compositions. TODO THis has to be streamlined
				if(entity == null || entity.length() == 0){
					entity = simpleLabel(entityId);
				}
				
				qualityId = rs.getString(6);
				quality = rs.getString(7);
				
				//TODO Streamline post compositions handling method here
				if(quality == null || quality.length() == 0){
					quality = semanticLabel(qualityId);
				}
				
				dto.setEntityId(entityId);
				dto.setEntity(entity);
				dto.setQualityId(qualityId);
				dto.setQuality(quality);
				
				dto.setPublication(rs.getString(8));
				
				dto.setCharText(rs.getString(9));
				dto.setCharComments(rs.getString(10));
				dto.setCharNumber(rs.getString(11));
				
				dto.setStateText(rs.getString(12));
				dto.setStateComments(rs.getString(13));
				
				dto.setCurators(rs.getString(14));
				
				results.add(dto);
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
	
	/**
	 * @author cartik
	 * @param queryString - the SQL query
	 * @param searchTerm - the search parameter for the SQL query. This term comes from the REST 
	 * service
	 * @return a collection of nodes
	 * @throws SQLException
	 * @PURPOSE The purpose of this method is to retrieve all the homolog information from
	 * the database and package them into a collection of nodes. All the retrieved information
	 * is added to these Nodes via links
	 */
	public Collection<HomologDTO> executeHomologyQueryAndAssembleResults(String searchTerm) 
		throws SQLException{
		Collection<HomologDTO> results = new ArrayList<HomologDTO>();
		PreparedStatement pStmt = null;
		
		try{
			pStmt = conn.prepareStatement(queries.getHomologyQuery());
			for(int i = 1; i <= pStmt.getParameterMetaData().getParameterCount(); i++)
				pStmt.setString(i, searchTerm);
			log.trace(pStmt.toString());
			long startTime = System.currentTimeMillis();
			ResultSet rs = pStmt.executeQuery();
			long endTime = System.currentTimeMillis();
			log.trace("Query execution took  " + (endTime -startTime) + " milliseconds");
			while(rs.next()){
				HomologDTO dto = new HomologDTO(rs.getString(1) + "\t" + rs.getString(6));
				dto.setLhTaxonId(rs.getString(2));
				dto.setLhTaxon(rs.getString(3));
				dto.setRhTaxonId(rs.getString(7));
				dto.setRhTaxon(rs.getString(8));
				dto.setLhEntityId(rs.getString(4));
				dto.setLhEntity(rs.getString(5));
				dto.setRhEntityId(rs.getString(9));
				dto.setRhEntity(rs.getString(10));
				
				dto.setPublication(rs.getString(11));
				dto.setEvidenceCode(rs.getString(12));
				dto.setEvidence(rs.getString(13));
				
				results.add(dto);
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
	 * @param queryStr - this is provided by the invoking REST service. This is the SQL query to be executed
	 * @param searchTerm - the UID (eg: TAO:0000108) to search for. Also provided by the invoking REST service
	 * @param filterOptions - the rows retrieved by the query execution are filtered through these values which are provided
	 * by the invoking REST service
	 */

	public Collection<PhenotypeDTO> executeQueryAndAssembleResults(String queryStr, String searchTerm, Map<String, String> filterOptions) 
		throws SQLException{
		Collection<PhenotypeDTO> results = new ArrayList<PhenotypeDTO>();
		String entityLabel, entityId;
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
						PhenotypeDTO dto = new PhenotypeDTO(rs.getString(1));
						/*
						 * We keep track of both labels and uids, so we dont have to go looking for labels for uids
						 * at the REST resource level. The queries return everything anyways.
						 */
						dto.setTaxon(rs.getString(3));
						dto.setTaxonId(rs.getString(2));
						dto.setQuality(rs.getString(5));
						dto.setQualityId(rs.getString(4));
						dto.setCharacter(rs.getString(7));
						dto.setCharacterId(rs.getString(6));
						entityLabel = rs.getString(9);
						entityId = rs.getString(8);
						if(entityLabel == null){
							entityLabel = simpleLabel(entityId);
						}	
						dto.setEntity(entityLabel);
						dto.setEntityId(entityId);
						dto.setReifId(rs.getString(10));
						results.add(dto);
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
     * TAO:0001173 with 'Dorsal fin'. Carats (^) are replaced with the string 
     * " that is " (Paula Mabee, Email Comm. 061209) and underscores are replaced 
     * with white spaces.
     * Note - this method uses regular expressions which make assumptions about the node identifer.
     * Use the label() methods instead.
     * @param cd - CompositionalDescription. is of the form (<entity_uid>^ <rel_uid>(<entity_uid>)
     * @return label - is of the form (<entity_label> <rel_label> <entity_label>
     */
	@Deprecated
	public String resolveLabel(String cd){
		String label = cd.replaceAll("\\^", " that is ");
		label = label.replaceAll("\\(", " ");
		label = label.replaceAll("\\)", " ");
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
		label = label.trim();
		return label;
	}
	
	 /**
     * Return a label for any node in a shard. If the node has no label, a label is generated recursively based on the 
     * node's properties as a compositional descripton.
     * @param node The Node for which to return or generate a label.
     * @author jim balhoff
     */
    public String semanticLabel(Node node) {
        if (node.getLabel() != null) {
            return node.getLabel();
        } else {
            final CompositionalDescription desc = this.shard.getCompositionalDescription(node.getId(), false);
            final List<String> differentia = new ArrayList<String>();
            final Collection<CompositionalDescription> differentiaArguments = desc.getDifferentiaArguments();
            if (differentiaArguments == null) {
                log.error("No differentia arguments for: " + node.getId());
                return node.getId();
            }
            for (CompositionalDescription differentium : differentiaArguments) {
                final StringBuffer buffer = new StringBuffer();
                buffer.append(semanticLabel(differentium.getRelationId()));
                buffer.append("(");
                buffer.append(semanticLabel(differentium.getRestriction().getTargetId()));
                buffer.append(")");
                differentia.add(buffer.toString());
            }
            return semanticLabel(desc.getGenus().getNodeId()) + "("+ Collections.join(differentia, ", ") + ")";
        }
    }
    
    /**
     * Return a label for any node in a shard. If the node has no label, a label is generated recursively based on the 
     * node's properties as a compositional descripton.
     * @param id The identifier of a Node for which to return or generate a label.
     * @author jim balhoff
     */
    public String semanticLabel(String id) {
        final Node node = this.shard.getNode(id);
        if (node != null) {
            return semanticLabel(node);
        }
        else {
            return id;
        }
    }
    
    public String simpleLabel(Node node) {
        if (node.getLabel() != null) {
            return node.getLabel();
        } else {
            final CompositionalDescription desc = this.shard.getCompositionalDescription(node.getId(), false);
            final List<String> differentia = new ArrayList<String>();
            final Collection<CompositionalDescription> differentiaArguments = desc.getDifferentiaArguments();
            if (differentiaArguments == null) {
                log.error("No differentia arguments for: " + node.getId());
                return node.getId();
            }
            for (CompositionalDescription differentium : differentiaArguments) {
                final StringBuffer buffer = new StringBuffer();
                buffer.append(" of ");
                buffer.append(simpleLabel(differentium.getRestriction().getTargetId()));
                differentia.add(buffer.toString());
            }
            return simpleLabel(desc.getGenus().getNodeId()) + Collections.join(differentia, ", ");
        }
    }
    
    public String simpleLabel(String id) {
        final Node node = this.shard.getNode(id);
        return simpleLabel(node);
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
