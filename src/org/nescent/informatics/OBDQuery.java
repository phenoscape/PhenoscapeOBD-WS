package org.nescent.informatics;

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
import org.obd.query.AnnotationLinkQueryTerm;
import org.obd.query.QueryTerm;
import org.obd.query.Shard;
import org.obd.query.impl.OBDSQLShard;
import org.phenoscape.bridge.OBDModelBridge;
import org.purl.obo.vocab.RelationVocabulary;

/**
 * FIXME Class comment missing.
 */
public class OBDQuery {

	private static RelationVocabulary relationVocabulary = new RelationVocabulary();
	private final Shard shard;
	private Connection conn;
	private Map<String, Integer> relationNodeIds;
	
	public Logger log;

    /* FIXME this file seems like a bad place for these constants -
     * aren't they used by other classes too? shouldn't there an
     * OBDRelation or OBDConstants class?
     */
	private final static String IS_A_RELATION_ID = "OBO_REL:is_a"; 
	private final static String INHERES_IN_RELATION_ID = "OBO_REL:inheres_in";
	private final static String HAS_ALLELE_RELATION_ID = "PHENOSCAPE:has_allele";
	private final static String EXHIBITS_RELATION_ID = "PHENOSCAPE:exhibits";
	private final static String VALUE_FOR_RELATION_ID = "PHENOSCAPE:value_for";
	
	private String anatomyQuery; 			
	private String taxonQuery;
	private String geneQuery;  
	
        /**
         * FIXME Constructor and parameter documentation missing.
         */
	public OBDQuery(Shard shard, Connection conn, String[] queries){
		this(shard, conn);
			
		this.anatomyQuery = queries[0];
		this.taxonQuery = queries[1];
		this.geneQuery = queries[2];
		
                // FIXME the replacement strings should be constants,
                // should be documented, and should be in a different file.
                
                // FIXME this looks repetitive - rewrite this as a loop

                // FIXME why aren't we using the accessor method
                // (getRelationNodeIds()) here?
		anatomyQuery = anatomyQuery.replaceAll("___inheres_in", relationNodeIds.get("inheres_in").toString());
		taxonQuery = taxonQuery.replaceAll("___inheres_in", relationNodeIds.get("inheres_in").toString());
		geneQuery = geneQuery.replaceAll("___inheres_in", relationNodeIds.get("inheres_in").toString());
			
		anatomyQuery = anatomyQuery.replaceAll("___exhibits", relationNodeIds.get("exhibits").toString());
		taxonQuery = taxonQuery.replaceAll("___exhibits", relationNodeIds.get("exhibits").toString());
		geneQuery = geneQuery.replaceAll("___exhibits", relationNodeIds.get("exhibits").toString());
			
		anatomyQuery = anatomyQuery.replaceAll("___has_allele", relationNodeIds.get("has_allele").toString());
		taxonQuery = taxonQuery.replaceAll("___has_allele", relationNodeIds.get("has_allele").toString());
		geneQuery = geneQuery.replaceAll("___has_allele", relationNodeIds.get("has_allele").toString());
			
		anatomyQuery = anatomyQuery.replaceAll("___value_for", relationNodeIds.get("value_for").toString());
		taxonQuery = taxonQuery.replaceAll("___value_for", relationNodeIds.get("value_for").toString());
		geneQuery = geneQuery.replaceAll("___value_for", relationNodeIds.get("value_for").toString());
			
		anatomyQuery = anatomyQuery.replaceAll("___is_a", relationNodeIds.get("is_a").toString());
		taxonQuery = taxonQuery.replaceAll("___is_a", relationNodeIds.get("is_a").toString());
		geneQuery = geneQuery.replaceAll("___is_a", relationNodeIds.get("is_a").toString());

                // FIXME should print context to it's clear from the
                // logs what this is. Also, why are taxonQuery and
                // geneQuery not being debugged here too?
		log.debug(anatomyQuery);
	}
	
        /**
         * FIXME Constructor and parameter documentation missing.
         */
	public OBDQuery(Shard shard, Connection conn){
		this.shard = shard;
                // FIXME if the shard instanceof AbstractSQLShard,
                // could obtain the connection from the shard
		this.conn = conn;
			
		this.log = Logger.getLogger(this.getClass());
		
		relationNodeIds = new HashMap<String, Integer>();

                // FIXME why is this not using the relationship
                // constants defined above? At any rate, the keys need
                // to be constants or elements of an enumeration.

                // FIXME why are we assuming that we have an
                // OBDSQLShard or derived class here? And if that's
                // what we must have, why is it not the type requested
                // in the constructor argument declaration?

                // FIXME why aren't we using the accessor method
                // (getRelationNodeIds()) here?
		relationNodeIds.put("is_a", ((OBDSQLShard) this.shard).getNodeInternalId(IS_A_RELATION_ID));
		relationNodeIds.put("inheres_in", ((OBDSQLShard) this.shard).getNodeInternalId(INHERES_IN_RELATION_ID));
		relationNodeIds.put("has_allele", ((OBDSQLShard) this.shard).getNodeInternalId(HAS_ALLELE_RELATION_ID));
		relationNodeIds.put("exhibits", ((OBDSQLShard) this.shard).getNodeInternalId(EXHIBITS_RELATION_ID));
		relationNodeIds.put("value_for", ((OBDSQLShard) this.shard).getNodeInternalId(VALUE_FOR_RELATION_ID));
	}
	
	public Map<String, Integer> getRelationNodeIds(){
		return relationNodeIds;
	}
	
	public String getAnatomyQuery(){
		return anatomyQuery;
	}
	
	public String getTaxonQuery(){
		return taxonQuery;
	}
	
	public String getGeneQuery(){
		return geneQuery;
	}

        /**
         * FIXME Method and parameter documentation missing.
         */
    /* FIXME according to the code in filterNode(), the variable
     * filterOptions parameter isn't actually variable - if there are
     * not precisely 3 values, the result will be IndexOutOfBounds
     */
    /* FIXME poorly named method - this seems to be more like
     * fetchNodes(), or fetchPhenotypeNodes().
     */
	public Collection<Node> executeQuery(String queryStr, String searchTerm, String... filterOptions){
		Collection<Node> results = new ArrayList<Node>();
		String entityLabel;
                PreparedStatement pstmt = null;
		try{
			pstmt = conn.prepareStatement(queryStr);
			pstmt.setString(1, searchTerm);
			log.trace(pstmt.toString());
			long startTime = System.currentTimeMillis();
			ResultSet rs = pstmt.executeQuery();
			long endTime = System.currentTimeMillis();
			log.trace("Query execution took  " + (endTime -startTime) + " milliseconds");
			while(rs.next()){
                            
                            // FIXME it's completely obscure what is
                            // going on in this while() loop, and the
                            // method name doesn't provide any clue
                            // either. Please deobfuscate.

				if(!filterNode(filterOptions, new String[]{rs.getString(2), rs.getString(8), rs.getString(6)})){
					if(!rs.getString(2).contains("GENO")){
						Node phenotypeNode = new Node(rs.getString(1));
                // FIXME why is this not using the relationship
                // constants defined above?

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
						phenotypeNode.addStatement(taxonOrGeneSt);
						phenotypeNode.addStatement(taxonOrGeneIdSt);
						phenotypeNode.addStatement(stateSt);
						phenotypeNode.addStatement(stateIdSt);
						phenotypeNode.addStatement(characterSt);
						phenotypeNode.addStatement(characterIdSt);
						phenotypeNode.addStatement(entitySt);
						phenotypeNode.addStatement(entityIdSt);
						results.add(phenotypeNode);
					}
				}
			}
		}
		catch(SQLException sqle){
                    log.error(sqle);
                    // FIXME we don't really want to chain exceptions
                    // here but ideally just rethrow the SQLException
                    // so that a caller has more information on what
                    // went wrong, but that requires changing the
                    // method signature (it's a checked exception).
                    throw new RuntimeException(sqle);
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
         * FIXME Method and parameter documentation missing.
         */
    /*
     * FIXME Poorly method - calling code seems to be doing something
     * if the method returns false, and skip a row if the method
     * returns true - should this be isSkipRow() or isFilterOut()?
     * What if the two arrays are of unequal length? Nothing seems to
     * protect against that.
     */
	private boolean filterNode(String[] filterOptions, String[] ids) {
		for(int i = 0 ; i < filterOptions.length; i++){
			if(filterOptions[i] != null && !filterOptions[i].equals(ids[i])){
					return true;
			}
		}
		return false;
	}

        /**
         * FIXME Method and parameter documentation missing.
         */
	public String resolveLabel(String cd){
		String label = cd.replaceAll("\\^", " ");
		String oldLabel = label;
                // FIXME need to document what this pattern does - is
                // this trying to identify OBO-style term identifiers?
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
         * FIXME Parameter documentation and what the method does and
         * how it does it are missing.
         *
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
		results.addAll(this.shard.getStatementsWithSearchTerm(term, null, null, source, false, false));
		results.addAll(this.shard.getStatementsWithSearchTerm(null, null, term, source, false, false));
		return results;
	}
	
	/**
         * FIXME Parameter documentation and what the method does and
         * how it does it are missing.
         *
	 * @author cartik
	 * @param term
	 * @param options
	 * @return
	 * a method for term auto completions
	 */
	public Map<String, Collection<Node>> getCompletionsForSearchTerm(String term, boolean zfinOption, List<String> ontologyList, String... options){
		boolean bySynonymOption = Boolean.parseBoolean(options[0]);
		boolean byDefinitionOption = Boolean.parseBoolean(options[1]);

		Map<String, Collection<Node>> results = new HashMap<String, Collection<Node>>();

		Collection<Node> nodesByName = this.shard.getNodesForSearchTermByLabel(term, zfinOption, ontologyList);
		nodesByName = pruneNodesForUUID(nodesByName);
		
                // FIXME the keys should be constants that are visible
                // to calling classes, or elements of an enumeration
		results.put("name-matches", nodesByName);
		if(bySynonymOption){
			Collection<Node> nodesBySynonym = this.shard.getNodesForSearchTermBySynonym(term, zfinOption, ontologyList, true);
			nodesBySynonym = pruneNodesForUUID(nodesBySynonym);
			results.put("synonym-matches", nodesBySynonym);
		}
		if(byDefinitionOption){
			Collection<Node> nodesByDefinition = this.shard.getNodesForSearchTermByDefinition(term, zfinOption, ontologyList);
			nodesByDefinition = pruneNodesForUUID(nodesByDefinition);
			results.put("definition-matches", nodesByDefinition);
		}
		return results;
	}
		
	/**
         * FIXME Parameter documentation and what the method does and
         * how it does it are missing.
         *
	 * @author cartik
	 * @param nodes
	 * @return
	 * Another helper method for the auto completion method above to prune nodes that do not have ontology based identifiers
	 */
	
	
	public Collection<Node> pruneNodesForUUID(Collection<Node> nodes){
		Collection<Node> nodes2return = new ArrayList<Node>();
		for(Node node: nodes){
			if (!Pattern.matches(
					"[0-9a-z]+-[0-9a-z]+-[0-9a-z]+-[0-9a-z]+-[0-9a-z]+", node.getId())) { // avoid UUID class generated identifiers
				nodes2return.add(node);
			}
		}
		return nodes2return;
	}
	
	/**
         * FIXME Parameter documentation and what the method does and
         * how it does it are missing.
         *
	 * This method is designed to find statements that contain a
	 * specific relation-target combination
	 * @param term
	 * @return
	 */
	public Collection<Statement> getStatementsWithPredicateAndObject(String target, String relation){
		if(target == null || relation == null){
			throw new IllegalArgumentException("ERROR: Input parameter is null");
		}
		return this.shard.getStatementsWithSearchTerm(null, relation, target, null, false, false);
	}
	
	/**
         * FIXME Parameter documentation and what the method does and
         * how it does it are missing.
         *
	 * This method is designed to find specific targets of a node by traversing a specific relation
	 */
	public Collection<Statement> getStatementsWithSubjectAndPredicate(String subj, String pred){
		if(subj == null || pred == null){
			throw new IllegalArgumentException("ERROR: Input parameter is null");
		}
		return this.shard.getStatementsWithSearchTerm(subj, pred, null, null, false, false);
	}
	
}
