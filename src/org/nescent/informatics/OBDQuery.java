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

public class OBDQuery {

	private static RelationVocabulary relationVocabulary = new RelationVocabulary();
	private final Shard shard;
	private Connection conn;
	private Map<String, Integer> relationNodeIds;
	
	public Logger log;
	
	private static String IS_A_RELATION_ID = "OBO_REL:is_a"; 
	private static String INHERES_IN_RELATION_ID = "OBO_REL:inheres_in";
	private static String HAS_ALLELE_RELATION_ID = "PHENOSCAPE:has_allele";
	private static String EXHIBITS_RELATION_ID = "PHENOSCAPE:exhibits";
	private static String VALUE_FOR_RELATION_ID = "PHENOSCAPE:value_for";
	
	private String anatomyQuery; 			
	private String taxonQuery;
	private String geneQuery;  
	
	public OBDQuery(Shard shard, Connection conn, String[] queries){
		this(shard, conn);
			
		this.anatomyQuery = queries[0];
		this.taxonQuery = queries[1];
		this.geneQuery = queries[2];
		
		anatomyQuery = anatomyQuery.replaceAll("___inheres_in", "" + relationNodeIds.get("inheres_in"));
		taxonQuery = taxonQuery.replaceAll("___inheres_in", "" + relationNodeIds.get("inheres_in"));
		geneQuery = geneQuery.replaceAll("___inheres_in", "" + relationNodeIds.get("inheres_in"));
			
		anatomyQuery = anatomyQuery.replaceAll("___exhibits", "" + relationNodeIds.get("exhibits"));
		taxonQuery = taxonQuery.replaceAll("___exhibits", "" + relationNodeIds.get("exhibits"));
		geneQuery = geneQuery.replaceAll("___exhibits", "" + relationNodeIds.get("exhibits"));
			
		anatomyQuery = anatomyQuery.replaceAll("___has_allele", "" + relationNodeIds.get("has_allele"));
		taxonQuery = taxonQuery.replaceAll("___has_allele", "" + relationNodeIds.get("has_allele"));
		geneQuery = geneQuery.replaceAll("___has_allele", "" + relationNodeIds.get("has_allele"));
			
		anatomyQuery = anatomyQuery.replaceAll("___value_for", "" + relationNodeIds.get("value_for"));
		taxonQuery = taxonQuery.replaceAll("___value_for", "" + relationNodeIds.get("value_for"));
		geneQuery = geneQuery.replaceAll("___value_for", "" + relationNodeIds.get("value_for"));
			
		anatomyQuery = anatomyQuery.replaceAll("___is_a", "" + relationNodeIds.get("is_a"));
		taxonQuery = taxonQuery.replaceAll("___is_a", "" + relationNodeIds.get("is_a"));
		geneQuery = geneQuery.replaceAll("___is_a", "" + relationNodeIds.get("is_a"));
			
		log.debug(anatomyQuery);
	}
	
	public OBDQuery(Shard shard, Connection conn){
		this.shard = shard;
		this.conn = conn;
			
		this.log = Logger.getLogger(this.getClass());
		
		relationNodeIds = new HashMap<String, Integer>();
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
	public Collection<Node> executeQuery(String queryStr, String searchTerm, String... filterOptions){
		Collection<Node> results = new ArrayList<Node>();
		String entityLabel;
		try{
			PreparedStatement pstmt = conn.prepareStatement(queryStr);
			pstmt.setString(1, searchTerm);
			log.trace(pstmt.toString());
			long startTime = System.currentTimeMillis();
			ResultSet rs = pstmt.executeQuery();
			long endTime = System.currentTimeMillis();
			log.trace("Query execution took  " + (endTime -startTime) + " milliseconds");
			while(rs.next()){
				if(!filterNode(filterOptions, new String[]{rs.getString(2), rs.getString(8), rs.getString(6)})){
					if(searchTerm.contains("GENE") || !rs.getString(2).contains("GENO")){
						Node phenotypeNode = new Node(rs.getString(1));
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
			sqle.printStackTrace();
		}
		return results;
	}
	
	private boolean filterNode(String[] filterOptions, String[] ids) {
		for(int i = 0 ; i < filterOptions.length; i++){
			if(filterOptions[i] != null && !filterOptions[i].equals(ids[i])){
					return true;
			}
		}
		return false;
	}

	public String resolveLabel(String cd){
		String label = cd;
		label = label.replaceAll("\\^", " ");
		String oldLabel = label;
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
	 * The methods in the first section below have been developed for the Phenoscape application to be demo'ed
	 * at the SICB workshop in Boston, MA in January 2009
	 */
	
	/**
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
//		results.addAll(this.shard.getStatementsWithSearchTerm(null, term, null, null, false, false));
		results.addAll(this.shard.getStatementsWithSearchTerm(null, null, term, source, false, false));
		return results;
	}
	
	/**
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
	 * This method is designed to find statements that contain a specific relation-target combination
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
	 * This method is designed to find specific targets of a node by traversing a specific relation
	 */
	public Collection<Statement> getStatementsWithSubjectAndPredicate(String subj, String pred){
		if(subj == null || pred == null){
			throw new IllegalArgumentException("ERROR: Input parameter is null");
		}
		return this.shard.getStatementsWithSearchTerm(subj, pred, null, null, false, false);
	}
	
	/**
	 * These methods below were developed primarily for the PhenoscapeWeb project demo'ed at the Second
	 * Data Roundup in South Dakota in September 2008.
	 * Subsequently, the phenotype (Entity Quality combination) search has been speeded up by adding specific 
	 * upgrades to the OBDSQLShard and the WhereClause classes in the OBDAPI and the BBOP projects respectively 
	 * Apart from this, nothing else has been changed in this section - Cartik (Dec 15, 2008)
	 */
	
	/**
	 * 
	 * @param taxon
	 * @param entity
	 * @param quality
	 * @return - a set (collection) of string arrays that represent every
	 *         statement that was found in the OBD database about the search
	 *         string which could be a taxon, or an entity, or a quality
	 * @throws IllegalArgumentException
	 *             This method is the controller that decides which of the more
	 *             specific methods are to be invoked to satisfy a client
	 *             request.
	 */

	public Set<Statement> genericSearch(String taxon, String entity,
			String quality) throws IllegalArgumentException {
		if (taxon == null && entity == null && quality == null) {
			throw new IllegalArgumentException("ERROR: All input parameters "
					+ "to the search method are NULL");
		}

		if (taxon != null && entity == null && quality == null) {
			return getStatementsByTaxon(taxon);
		} else if (taxon == null && entity != null && quality == null) {
			return getStatementsByCharacter(entity);
		} else if (taxon == null && entity == null && quality != null) {
			return getStatementsByState(quality);
		} else if (taxon != null && entity != null && quality == null) {
			return getStatementsByTaxonAndCharacter(taxon, entity);
		} else if (taxon == null && entity != null && quality != null) {
			return getStatementsByCharacterAndState(entity, quality);
		} else if (taxon != null && entity == null && quality != null) {
			return getStatementsByTaxonAndState(taxon, quality);
		} else {
			return isStatementFound(taxon, entity, quality);
		}
	}

	/**
	 * @author Cartik1.0
	 * @param Prints
	 *            out annotations about every data matrix in the database
	 */
	public void getStatementsAboutNexusMatrix() {

		QueryTerm dataSetQuery = new AnnotationLinkQueryTerm(relationVocabulary
				.instance_of(), OBDModelBridge.DATASET_TYPE_ID);

		Collection<Node> matrixNodes = this.shard.getNodesByQuery(dataSetQuery);

		for (Node matrixNode : matrixNodes) {
			String matrixId = matrixNode.getId();
			Collection<Statement> stmts = this.shard.getStatementsByNode(matrixId);
			for (Statement stmt : stmts) {
				Collection<Statement> subs = stmt.getSubStatements();
				String relation = stmt.getRelationId();
				String object = stmt.getTargetId().contains(":") ? stmt
						.getTargetId() : this.shard.getNode(stmt.getTargetId())
						.getLabel();
				System.out.println("MATRIX: " + matrixId + "\tRELATION: "
						+ relation + "\tOBJECT: " + object);
				for (Statement sub : subs) {
					System.out.println("subREL: " + sub.getRelationId()
							+ "\tsubTARG: " + sub.getTargetId());
				}
			}
		}
	}

	/**
	 * @author Cartik1.0
	 * @param character
	 * @return a set of string arrays. Each member of the array contains the
	 *         source and target node names (and identifiers), apart from the
	 *         directed edge Queries the OBD database for annotations that
	 *         contain the queried character in them. Results are returned as a
	 *         Set (Collection) of string arrays
	 */
	public Set<Statement> getStatementsByCharacter(String character) {

		Set<Statement> taxa = new HashSet<Statement>();

	
		/*
		QueryTerm charAnnotQuery = new AnnotationLinkQueryTerm(character);
		QueryTerm charQuery = new LinkQueryTerm(character);

		Collection<Statement> annots = obdsql2
				.getStatementsByQuery(charAnnotQuery);
		for (Statement annot : annots) {
			taxa.add(annot);
		}
		Collection<Statement> links = obdsql2.getStatementsByQuery(charQuery);
		for (Statement link : links) {
			taxa.add(link);
		}

		Collection<Statement> edges = obdsql2.getStatementsByNode(character);
		for (Statement edge : edges) {
			taxa.add(edge);
		}
		
		*/
		
		for (Statement s : this.shard.getStatementsForNode(character,false)){
			if (!s.isAnnotation()){
				taxa.add(s);
			}
		}
		
		for (Statement s : this.shard.getStatementsForTarget(character,false)){
			if (!s.isAnnotation()){
				taxa.add(s);
			}
		}
		
		taxa.addAll(this.shard.getStatementsWithSearchTerm(null, null, character, null, false, null));
		taxa.addAll(this.shard.getStatementsWithSearchTerm(character, null, null, null, false, null));
		taxa.addAll(this.shard.getAnnotationStatementsForNode(character, null, null));
		taxa.addAll(this.shard.getAnnotationStatementsForAnnotatedEntity(character, null, null));
		

		return taxa;
	}



	/**
	 * @author Cartik1.0
	 * @param taxon
	 *            - the taxon identifier such as TTO:1001979
	 * @return A Set(Collection) of String arrays containing assertions about
	 *         the taxon being searched for. Only assertions are returned,
	 *         trivial inferences are not
	 */
	public Set<Statement> getStatementsByTaxon(String taxon) {

		Set<Statement> statements = new HashSet<Statement>();
	
		/*
		Collection<Statement> stmts = obdsql2.getStatementsByNode(taxon);

		for (Statement stmt : stmts) {
			statements.add(stmt);
		}
		
		*/
		
		for (Statement s : this.shard.getStatementsForNode(taxon,false)){
			if (!s.isAnnotation()){
				statements.add(s);
			}
		}
		
		for (Statement s : this.shard.getStatementsForTarget(taxon,false)){
			if (!s.isAnnotation()){
				statements.add(s);
			}
		}
		
		statements.addAll(this.shard.getAnnotationStatementsForNode(taxon, null, null));
		statements.addAll(this.shard.getAnnotationStatementsForAnnotatedEntity(taxon, null, null));

		return statements;
	}

	/**
	 * @author Cartik1.0
	 * @param patoId
	 *            - A PATO identifier for a state
	 * @return a Set (Collection) of String arrays representing all found
	 *         assertions about the state in the ODB database
	 */
	public Set<Statement> getStatementsByState(String patoId) {

		Set<Statement> statements = new HashSet<Statement>();

		/*
		Collection<Statement> stmts = obdsql2.getStatementsByNode(patoId);
		for (Statement stmt : stmts) {
			statements.add(stmt);
		}

		QueryTerm qualAnnotQuery = new AnnotationLinkQueryTerm(patoId);
		QueryTerm qualQuery = new LinkQueryTerm(patoId);

		Collection<Statement> annots = obdsql2
				.getStatementsByQuery(qualAnnotQuery);

		for (Statement annot : annots) {
			statements.add(annot);
		}

		Collection<Statement> links = obdsql2.getStatementsByQuery(qualQuery);
		for (Statement link : links) {
			statements.add(link);
		}
		
		*/
		
		for (Statement s : this.shard.getStatementsForNode(patoId,false)){
			if (!s.isAnnotation()){
				statements.add(s);
			}
		}
		
		for (Statement s : this.shard.getStatementsForTarget(patoId,false)){
			if (!s.isAnnotation()){
				statements.add(s);
			}
		}
		
		statements.addAll(this.shard.getStatementsWithSearchTerm(null, null, patoId, null, false, null));
		statements.addAll(this.shard.getStatementsWithSearchTerm(patoId, null, null, null, false, null));
		statements.addAll(this.shard.getAnnotationStatementsForNode(patoId, null, null));
		statements.addAll(this.shard.getAnnotationStatementsForAnnotatedEntity(patoId, null, null));

		return statements;
	}

	/**
	 * @author Cartik1.0
	 * @param taxon
	 *            - TTO term
	 * @param character
	 *            - TAO term
	 * @return set (collection) of string arrays for every statement in the ODB
	 *         that has both the TTO term and the TAO term in it
	 */
	public Set<Statement> getStatementsByTaxonAndCharacter(String taxon,
			String character) {
		
	

		Set<Statement> statements = new HashSet<Statement>();
		
		for (Statement s : this.shard.getStatementsForNode(taxon,false)){
			if (!s.isAnnotation()){
				if(s.getTargetId().contains(character)){
					statements.add(s);
				}
			}
		}
		
		for (Statement s : this.shard.getStatementsForTarget(taxon,false)){
			if (!s.isAnnotation()){
				if(s.getTargetId().contains(character)){
					statements.add(s);
				}
			}
		}
		
		for(Statement s : this.shard.getAnnotationStatementsForNode(taxon, null, null)){
			if(s.getTargetId().contains(character)){
				statements.add(s);
			}
		}
		
		for(Statement s : this.shard.getAnnotationStatementsForAnnotatedEntity(taxon, null, null)){
			if(s.getTargetId().contains(character)){
				statements.add(s);
			}
		}
		/*
		Collection<Statement> stmts = obdsql2.getStatementsByNode(taxon);
		for (Statement stmt : stmts) {
			if (stmt.getTargetId().contains(character)) {
					statements.add(stmt);
			}
		} */

		return statements;
	}

	/**
	 * @author Cartik1.0
	 * @param character - TAO term
	 * @param state - PATO term
	 * @return set (collection) of string arrays for every statement in the ODB
	 *         that has both the PATO term and the TAO term in it
	 */
	public Set<Statement> getStatementsByCharacterAndState(String character,
			String state) {
	
		Set<Statement> statements = new HashSet<Statement>();
		
//		CompositionalDescription cd = new CompositionalDescription(Predicate.INTERSECTION);
//		cd.addArgument(state);
//		cd.addArgument(relationVocabulary.inheres_in(), character);
		
//		System.out.println("CD: " + cd);
		
//		CompositionalDescriptionQueryTerm cdqt = new CompositionalDescriptionQueryTerm(cd);
		
//		Collection<Statement> stmts = obdsql.getStatementsByQuery(new LinkQueryTerm(cdqt));
		
		String phenotype = state + "%inheres%" + character;
		Collection<Statement> stmts = this.shard.getStatementsWithSearchTerm(null, null, phenotype, null, false, null);
		for (Statement stmt : stmts) {
			
			if (stmt.toString().contains(state) && 
					stmt.toString().contains(character) && !stmt.getNodeId().contains("_")){	
				statements.add(stmt);
			}
		}
		return statements;
	}

	/**
	 * @author Cartik1.0
	 * @param taxon - TTO term
	 * @param state - PATO term
	 * @return set (collection) of string arrays for every statement in the ODB
	 *         that has both the TTO term and the PATO term in it
	 */
	public Set<Statement> getStatementsByTaxonAndState(String taxon, String state) {
		
	
		 
		Set<Statement> statements = new HashSet<Statement>();
		/*Collection<Statement> stmts = obdsql2.getStatementsByNode(taxon);
		for(Statement stmt : stmts){
			if(stmt.getTargetId().contains(state)){
				statements.add(stmt);
			}
		}*/
		for (Statement s : this.shard.getStatementsForNode(taxon,false)){
			if (!s.isAnnotation()){
				if(s.getTargetId().contains(state)){
					statements.add(s);
				}
			}
		}
		
		for (Statement s : this.shard.getStatementsForTarget(taxon,false)){
			if (!s.isAnnotation()){
				if(s.getTargetId().contains(state)){
					statements.add(s);
				}
			}
		}
		
		for(Statement s : this.shard.getAnnotationStatementsForNode(taxon, null, null)){
			if(s.getTargetId().contains(state)){
				statements.add(s);
			}
		}
		
		for(Statement s : this.shard.getAnnotationStatementsForAnnotatedEntity(taxon, null, null)){
			if(s.getTargetId().contains(state)){
				statements.add(s);
			}
		}

		return statements;
	}

	
	/**
	 * @author Cartik1.0
	 * @param taxon - TTO term
	 * @param entity - TAO term
	 * @param quality - PATO term
	 * @return - A set with one string array showing the searched items are part of one sentence in the
	 * ODB database.
	 */
	public Set<Statement> isStatementFound(String taxon, String entity,
			String quality) {
		
		Set<Statement> statements = new HashSet<Statement>();

		Collection<Statement> stmts = this.shard.getStatementsByNode(taxon);
		for (Statement stmt : stmts) {
			if (stmt.getTargetId().contains(entity)
					&& stmt.getTargetId().contains(quality)) {
				statements.add(stmt);
			}
		}
	
		return statements;
	}
}
