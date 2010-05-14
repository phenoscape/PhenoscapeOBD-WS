package org.phenoscape.obd.query;

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

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.obd.model.CompositionalDescription;
import org.obd.model.Node;
import org.obd.query.Shard;
import org.obd.ws.resources.AutoCompleteResource;
import org.phenoscape.util.AnnotationDTO;
import org.phenoscape.util.Collections;
import org.phenoscape.util.HomologDTO;
import org.phenoscape.util.PhenotypeAndAnnotatedSubtaxonCountDTO;
import org.phenoscape.util.PhenotypeDTO;
import org.phenoscape.util.Queries;
import org.phenoscape.ws.application.PhenoscapeWebServiceApplication;

/**
 * This class interfaces with methods from the OBDAPI, specifically
 * in the Shard class hierarchy, and invokes these methods. Methods
 * defined in this class return their results to invoking REST 
 * resources
 */

public class OBDQuery {

	private final Shard shard;
	public Logger log;
	private Queries queries;
	
	public static enum AutoCompletionMatchTypes{
		LABEL_MATCH, SYNONYM_MATCH, DEFINITION_MATCH
	};

	/** A structure which maps ontology prefixes to their 
	 * default namespaces */
	private Map<String, Set<String>> prefixToDefaultNamespacesMap;
	/** A structure to map default namespaces of ontologies to their
	 * node ids in the database */
	private Map<String, String> defaultNamespaceToNodeIdMap;
	/** An enumeration of the possible match types for the 
	 * {@see #getAutocompletionsForSearchTerm} method	 */
	
	/** GETTER for the map from default namespaces of ontologies 
	 * to their node ids in the database */
	public Map<String, String> getDefaultNamespaceToNodeIdMap() {
		return defaultNamespaceToNodeIdMap;
	}
	/** GETTER for the map from ontology prefixes to default namespaces
	 */
	public Map<String, Set<String>> getPrefixToDefaultNamespacesMap() {
		return prefixToDefaultNamespacesMap;
	}
	
	/**
	 * Mapping for how to represent relations used in post-comp differentia when generating a human-readable label.
	 * If the relation is not in this map, use "of".
	 */
	private static final Map<String, String> POSTCOMP_RELATIONS = new HashMap<String, String>();
    static {
        POSTCOMP_RELATIONS.put("OBO_REL:connected_to", "on");
        POSTCOMP_RELATIONS.put("connected_to", "on");
        POSTCOMP_RELATIONS.put("anterior_to", "anterior to");
        POSTCOMP_RELATIONS.put("BSPO:0000096", "anterior to");
        POSTCOMP_RELATIONS.put("posterior_to", "posterior to");
        POSTCOMP_RELATIONS.put("BSPO:0000099", "posterior to");
        POSTCOMP_RELATIONS.put("adjacent_to", "adjacent to");
        POSTCOMP_RELATIONS.put("OBO_REL:adjacent_to", "adjacent to");
    }

	/**
	 * This is the default constructor. 
	 * @param shard = the shard to use. THis has to be an AbstractSQLShard to use the connection
	 * it comes with
	 * @throws SQLException 
	 */
	public OBDQuery(Shard shard) throws SQLException{
		this.shard = shard;
		this.log = Logger.getLogger(this.getClass());
	}

	/**
	 * Overloaded constructor. This will optimize the use of the Queries object throughout 
	 * the application
	 * @param shard - the shard to use
	 * @param queries - the instance of the Queries class
	 * @throws SQLException
	 */
	public OBDQuery(Shard shard, Queries queries) throws SQLException{
		this(shard);
		this.queries = queries;
	}
	
	/**
	 * This constructor is meant for use by the auto completion and term info service. This is 
	 * because this uses maps that are passed to it by the invoking service. These services
	 * obtain the maps from the application context  
	 * @param shard
	 * @param queries
	 * @param nsToSourceIdMap 
	 * @param prefixToNSMap
	 * @throws SQLException
	 */
	public OBDQuery(Shard shard, Queries queries, Map<String, String> nsToSourceIdMap, 
			Map<String, Set<String>> prefixToNSMap) throws SQLException{
		this(shard, queries);
		this.defaultNamespaceToNodeIdMap = nsToSourceIdMap;
		this.prefixToDefaultNamespacesMap = prefixToNSMap;
	}
	
	public List<PhenotypeAndAnnotatedSubtaxonCountDTO> executeQueryForSquarifiedTaxonMapResource(String taxonUID) throws SQLException {
		
		List<PhenotypeAndAnnotatedSubtaxonCountDTO> results = 
					new ArrayList<PhenotypeAndAnnotatedSubtaxonCountDTO>();
		PhenotypeAndAnnotatedSubtaxonCountDTO pascDTO;
		String id, name;
		int phenotypeCount, subtaxonCount;
		PreparedStatement ps1 = null, ps2 = null;
		final Connection conn = this.createConnection();
		try {
			ps1 = conn.prepareStatement(queries.getQueryForSquarifiedTaxonMapResource2());
			ps1.setString(1, taxonUID);
			ResultSet rs1 = ps1.executeQuery();
			while(rs1.next()){
				id = rs1.getString(1);
				name = rs1.getString(2);
				phenotypeCount = rs1.getInt(3);
				subtaxonCount = rs1.getInt(4);
				
				pascDTO = new PhenotypeAndAnnotatedSubtaxonCountDTO(id, name);
				pascDTO.setPhenotypeCount(phenotypeCount);
				pascDTO.setSubtaxonCount(subtaxonCount);
				results.add(pascDTO);
			}
			
			ps2 = conn.prepareStatement(queries.getQueryForSquarifiedTaxonMapResource1());
			ps2.setString(1, taxonUID);
			ResultSet rs2 = ps2.executeQuery();
			while(rs2.next()){
				id = rs2.getString(1);
				name = rs2.getString(2);
				phenotypeCount = rs2.getInt(3);
				subtaxonCount = rs2.getInt(4);
				
				pascDTO = new PhenotypeAndAnnotatedSubtaxonCountDTO(id, name);
				pascDTO.setPhenotypeCount(phenotypeCount);
				pascDTO.setSubtaxonCount(subtaxonCount);
				results.add(pascDTO);
			}
		} catch (SQLException sqle) {
			log.error(sqle);
			throw sqle;
		}
		finally {
			if (ps1 != null) {
				ps1.close();
			}
			if(ps2 != null) {
				ps2.close();
			}
			if (conn != null) {
				conn.commit();
				conn.close();
			}
		}
		return results;
	}
	
	/**
	 * This query returns the timestamp when the database was refreshed
	 * @return the timestamp
	 * @throws SQLException
	 */
	public String executeTimestampQuery() throws SQLException{
		String timestamp = "2009-07-24";
		PreparedStatement pStmt = null;
		final Connection conn = this.createConnection();
		try{
			pStmt = conn.prepareStatement(queries.getTimestampQuery());
			log.trace(pStmt.toString());
			long startTime = System.currentTimeMillis();
			ResultSet rs = pStmt.executeQuery();
			long endTime = System.currentTimeMillis();
			log.trace("Query execution took  " + (endTime -startTime) + " milliseconds");
			if(rs.next()){
				timestamp = rs.getString(1);
			}
			if(timestamp.contains("_"))
				timestamp = timestamp.substring(0, timestamp.indexOf("_"));
		}
		catch(SQLException sqle){
			log.error(sqle);
			throw sqle;
		}
		finally {
			if (pStmt != null) {
				pStmt.close();
			}
			if(conn != null){
				conn.commit();
				conn.close();
			}
		}
		return timestamp;
	}
	
	/**
	 * @param reifLinkId - the search parameter for the SQL query. This is an INTEGER
	 * which is provided by the invoking REST service. 
	 * @return a collection of DTOs, one for each row returned by the query
	 * @throws SQLException
	 * The purpose of this query is to return all the free text data
	 * (metadata) associated with a given <TAXON><EXHIBITS><PHENOTYPE> assertion
	 * The metadata is packaged into a collection of Nodes which are then returned
	 * back
	 */
	public Collection<AnnotationDTO> executeFreeTextQueryAndAssembleResults(Integer reifLinkId) throws SQLException{
		
		Collection<AnnotationDTO> results = new ArrayList<AnnotationDTO>();
		PreparedStatement pStmt = null;
		
		String entityId, entity, qualityId, quality, relEntityId, relEntity;
		final Connection conn = this.createConnection();
		try{
			pStmt = conn.prepareStatement(queries.getFreeTextDataQuery());
			pStmt.setInt(1, reifLinkId);
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
				
				relEntityId = rs.getString(15);
				relEntity = rs.getString(16);
				
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
				if(relEntityId != null){
					if(relEntity == null)
						relEntity = simpleLabel(relEntityId);
					if(quality.trim().endsWith("from") || quality.trim().endsWith("to") || quality.trim().endsWith("with"))
						quality += " " + relEntity;
					else
						quality += " towards " + relEntity;
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
				pStmt.close(); 
			}
			if (conn != null) { conn.close(); }
		}
		
		return results;
	}
	
	/**
	 * @param searchTerm - the search parameter for the SQL query. This term comes from the REST 
	 * service
	 * @return a collection of nodes
	 * @throws SQLException
	 * The purpose of this method is to retrieve all the homolog information from
	 * the database and package them into a collection of nodes. All the retrieved information
	 * is added to these Nodes via links
	 */
	public Collection<HomologDTO> executeHomologyQueryAndAssembleResults(String searchTerm) 
		throws SQLException{
		Collection<HomologDTO> results = new ArrayList<HomologDTO>();
		PreparedStatement pStmt = null;
		final Connection conn = this.createConnection();
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
				pStmt.close(); 
				
			}
			if(conn != null){
                conn.commit();
                conn.close();
            }
		}
		return results;
	}
	
	/**@throws SQLException 
	 * The purpose of this method is to execute the given query with set search term and assemble the results into a 
	 * makeshift persistence layer. The persistence layer is a collection of Data Transfer Objects. Each row is packaged into 
	 * a single PhenotypeDTO object. The phenotype is the central entity in this querying procedure. 
	 * Therefore, the IDs of the each node are set to be the retrieved phenotype. The method assembles
	 * these DTOs into a collection, which are returned
	 * @param queryStr - this is provided by the invoking REST service. This is the SQL query to be executed
	 * @param searchTerm - the UID (eg: TAO:0000108) to search for. Also provided by the invoking REST service
	 * @param filterOptions - the rows retrieved by the query execution are filtered through these values which are provided
	 * by the invoking REST service
	 */

	public Collection<PhenotypeDTO> executeQueryAndAssembleResults(String queryStr, String searchTerm) 
		throws SQLException{
		/** <a> annotationToReifsMap </a> 
		 * This new map has been added tp consolidate reif ids for a TAXON to PHENOTYPE assertion*/
		Map<PhenotypeDTO, Set<String>> annotationToReifsMap = new HashMap<PhenotypeDTO, Set<String>>();
		final Connection conn = this.createConnection();
		String entityLabel, entityId;
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement(queryStr);
			for(int i = 1; i <= pstmt.getParameterMetaData().getParameterCount(); i++)
				pstmt.setString(i, searchTerm);
			log.trace(pstmt.toString());
			long startTime = System.currentTimeMillis();
			ResultSet rs = pstmt.executeQuery();
			long endTime = System.currentTimeMillis();
			log.trace("Query execution took  " + (endTime -startTime) + " milliseconds");
			while(rs.next()){
				PhenotypeDTO dto = new PhenotypeDTO(rs.getString(1));
				
				dto.setTaxon(rs.getString(3));
				dto.setTaxonId(rs.getString(2));
				dto.setQuality(rs.getString(5));
				dto.setQualityId(rs.getString(4));
				dto.setCharacter(rs.getString(7));
				dto.setCharacterId(rs.getString(6));
				entityLabel = rs.getString(9);
				entityId = rs.getString(8);
				dto.setEntity(entityLabel);
				dto.setEntityId(entityId);
				String reif = rs.getString(10);
				dto.setNumericalCount(rs.getString(11));
				dto.setRelatedEntityId(rs.getString(12));
				dto.setRelatedEntity(rs.getString(13));
				dto.setPublication(rs.getString(14));
				
				//we collect all the reif ids associated with each DTO object
				Set<String> reifs = annotationToReifsMap.get(dto);
				if(reifs == null)
					reifs = new HashSet<String>();
				reifs.add(reif);
				annotationToReifsMap.put(dto, reifs);
			}
			for(PhenotypeDTO dto : annotationToReifsMap.keySet()){
				//here we reassign the reif ids to the DTO
				for(String reif : annotationToReifsMap.get(dto)){
				    dto.addReifId(reif);
				}				
			}
		}
		catch(SQLException sqle){
			log.error(sqle);
			throw sqle;
		}
		finally {
			if (pstmt != null) {
				pstmt.close(); 
            }
			if(conn != null){
				conn.commit();
				conn.close();
			}
		}
		return annotationToReifsMap.keySet();
	}
	
	 /**
     * Return a label for any node in a shard. If the node has no label, a label is generated recursively based on the 
     * node's properties as a compositional descripton.
     * @param node The Node for which to return or generate a label.
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
    
    /**
     * @param node - a post composition node
     * @return - a human readable label generated from the post composition
     */
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
                final String relationID = differentium.getRestriction().getRelationId();
                final String relationSubstitute;
                if (POSTCOMP_RELATIONS.containsKey(relationID)) {
                    relationSubstitute = POSTCOMP_RELATIONS.get(relationID);
                } else {
                    relationSubstitute = "of";
                }
                final StringBuffer buffer = new StringBuffer();
                buffer.append(" ");
                buffer.append(relationSubstitute);
                buffer.append(" ");
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
	 * This method uses the input search term from the form, and the 
	 * specified search options to first construct an SQL query and then 
	 * return the autocompletions returned by the query execution as a 
	 * data structure to the {@link AutoCompleteResource}
	 * @param searchTerm - the input search term from the form
	 * @param searchOptions - the search options specified in the form
	 * @return - a data structure which groups the autocompletions under
	 * different categories viz. label match, synonym match and definition
	 * match
	 * @throws SQLException
	 */
	public Map<String, List<List<String>>> getAutocompletionsForSearchTerm(String searchTerm, 
			Map<String, String> searchOptions) throws SQLException{
		List<String> ontologySourceIds = createSourceIdListFromOntologyOptions(searchOptions);
		String query = 
			constructAutocompleteQueryFromSearchTermAndSearchOptions(searchTerm, searchOptions);
		return executeAutocompletionQueryAndProcessResults(query, ontologySourceIds);
	}
	
	private Connection createConnection() throws SQLException {
	    final DataSource ds = (DataSource)(PhenoscapeWebServiceApplication.getCurrent().getContext().getAttributes().get(PhenoscapeWebServiceApplication.DATA_SOURCE_KEY));
	    return ds.getConnection();
	}
	
	/**
	 * This method uses the input search term and input specified 
	 * search options to construct an SQL query, which will be executed by the 
	 * {@link #executeAutocompletionQueryAndProcessResults(String, List)} method
	 * @param searchTerm - the input search term (comes from the form)
	 * @param searchOptions - search options specified in the form
	 * @return a SQL query string 
	 */
	private String constructAutocompleteQueryFromSearchTermAndSearchOptions(String searchTerm,
			Map<String, String> searchOptions){
		String completedQueryString = "";
		
		String synonymOption = searchOptions.get("synonymOption");
		String definitionOption = searchOptions.get("definitionOption");
		
		String autocompleteLabelQuery = queries.getAutocompleteLabelQuery() + 
														"'" + searchTerm + "'";
		String autocompleteSynonymQuery = queries.getAutocompleteSynonymQuery() + 
														"'" + searchTerm + "'";
		String autocompleteDefinitonQuery = queries.getAutocompleteDefinitionQuery() + 
														"'" + searchTerm + "'";
		completedQueryString = autocompleteLabelQuery;
		
		if(Boolean.parseBoolean(synonymOption)){
			completedQueryString += " UNION " + autocompleteSynonymQuery;
		}
		if(Boolean.parseBoolean(definitionOption)){
			completedQueryString += " UNION " + autocompleteDefinitonQuery;
		}
		return completedQueryString;
	}
	
	/**
	 * This query executes the given query and iterates through the 
	 * returned result set to construct a data structure, which groups each
	 * result under one of label match, synonym match or definition match
	 * @param query - the SQL query, which is assembled by the 
	 * {@link #constructAutocompleteQueryFromSearchTermAndSearchOptions(String, Map)} method
	 * @return - a data structure which groups the autocompletions under
	 * different categories viz. label match, synonym match and definition
	 * match
	 * @throws SQLException 
	 * @throws SQLException
	 */
	private Map<String, List<List<String>>> executeAutocompletionQueryAndProcessResults(String query, List<String> sourceIdList) throws SQLException {
		
		Map<String, List<List<String>>> resultsFromDifferentCategories = new HashMap<String, List<List<String>>>();
		List<List<String>> labelMatches = new ArrayList<List<String>>();
		List<List<String>> synonymMatches = new ArrayList<List<String>>();
		List<List<String>> definitionMatches = new ArrayList<List<String>>();
		Connection conn = null;
        try {
            conn = this.createConnection();
            java.sql.Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            List<String> row;
            String uid, label, definition, synonym, sourceId;
            while (rs.next()) {
                row = new ArrayList<String>();
                uid = rs.getString(1);
                label = rs.getString(2);
                synonym = rs.getString(3);
                definition = rs.getString(4);
                sourceId = rs.getString(5);
                
                if(!uid.contains("GENE")){ //GENEs dont have source ids
                    if(isToBeFiltered(sourceId, sourceIdList))
                        continue;
                }
                row.add(uid);
                row.add(label);
                row.add(synonym);
                row.add(definition);
                
                if(definition != null && definition.length() > 0)
                    definitionMatches.add(row);
                else if(synonym != null && synonym.length() > 0)
                    synonymMatches.add(row);
                else
                    labelMatches.add(row);
            }
            resultsFromDifferentCategories.put(AutoCompletionMatchTypes.LABEL_MATCH.name(), 
                                    labelMatches);
            resultsFromDifferentCategories.put(AutoCompletionMatchTypes.SYNONYM_MATCH.name(), 
                                    synonymMatches);
            resultsFromDifferentCategories.put(AutoCompletionMatchTypes.DEFINITION_MATCH.name(), 
                                    definitionMatches);
        } catch (SQLException e) {
            throw(e);
        } finally {
            if (conn != null) {
                conn.commit();
                conn.close();
            }
        }	
		return resultsFromDifferentCategories;
	}
	
	/**
	 * This method creates a list of source ids from the database
	 * given the list of ontologies to be searched
	 * @param searchOptions - the search options input from the 
	 * form. This includes the list of ontologies to be searched 
	 * @return a list of source ids for every ontology in the list
	 */
	private List<String> createSourceIdListFromOntologyOptions(
			Map<String, String> searchOptions){
		List<String> sourceIdList = new ArrayList<String>();
		String ontologyOption = searchOptions.get("ontologyOption");
		if(ontologyOption != null && ontologyOption.length() > 0){
			for(String prefix : ontologyOption.split(",")){
				if(prefixToDefaultNamespacesMap.get(prefix) != null){
					for(String namespace : 
						prefixToDefaultNamespacesMap.get(prefix)){
						sourceIdList.add(this.defaultNamespaceToNodeIdMap.get(namespace));
					}
				}
			}
		}
		return sourceIdList;
	}
	/**
	 * This method decides if a result is to be
	 * filtered given the source id.
	 * The input source id is compared with the
	 * list of source ids of the ontologies whose terms are
	 * to be included in the results
	 * @param sourceId - the source id of the result
	 * @param sourceIdList - the list of source ids corr.
	 * to the ontologies whose terms are to be included 
	 * in the results
	 * @return a boolean to indicate if this result is to be
	 * filtered
	 */
	private boolean isToBeFiltered(String sourceId, 
			List<String> sourceIdList) {
		if(sourceId == null) return true; 
		if(sourceIdList != null && sourceIdList.size() > 0){
			if(sourceIdList.contains(sourceId))
				return false;
			else
				return true;
		}
		return false;
	}
	
	@Deprecated
	/**
     * The purpose of this method is to return matching nodes for
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