package org.obd.ws.resources;

import java.io.IOException;
import java.sql.Connection;
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

import org.json.JSONException;
import org.json.JSONObject;
import org.nescent.informatics.OBDQuery;
import org.obd.model.Node;
import org.obd.model.Statement;
import org.obd.query.Shard;
import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Reference;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.Variant;

public class AnatomyResource extends Resource {

	private final String termId;
	private JSONObject jObjs;
	private Shard obdsql;
	private Connection conn;
	private OBDQuery obdq;
	private Set<String> taxa;
	private Set<String> genotypes;
	private Set<String> genes;
	private List<String> characters;
	private Set<String> qualitiesForCharacter;
	
	private Map<String, Set<String>> qualityToTaxonMap;
	private Map<String, Set<String>> qualityToGenotypeMap;
	private Map<String, Set<String>> qualityToGeneMap;

	private Map<String, Set<String>> characterToQualityMap;
	private Map<String, String> qualitiesToCharacterMap;
	
	private Map<String, Integer> statementCountByTaxa;
	private Map<String, Integer> statementCountByGenotype;
	private Map<String, Integer> statementCountByGene;
	
	private Set<String> geneSet, taxonSet;
	
	private int annotationCount;

	private final String OBOOWL_SUBSET_RELATION = "oboInOwl:inSubset";
	private final String IS_A_RELATION = "OBO_REL:is_a";
	private final String EXHIBITS_RELATION = "PHENOSCAPE:exhibits";
	private final String HAS_ALLELE_RELATION = "PHENOSCAPE:has_allele";

	private final String VALUE_SLIM_STRING = "value_slim";
	
	
	public AnatomyResource(Context context, Request request, Response response) {
		super(context, request, response);

		this.obdsql = (Shard) this.getContext().getAttributes().get("shard");
		this.conn = (Connection)this.getContext().getAttributes().get("conn");
		this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
		// this.getVariants().add(new Variant(MediaType.TEXT_HTML));
		this.termId = Reference.decode((String) (request.getAttributes()
				.get("termID")));
		String aq = (String)this.getContext().getAttributes().get("anatomyQuery");
		String tq = (String)this.getContext().getAttributes().get("taxonQuery");
		String gq = (String)this.getContext().getAttributes().get("geneQuery");
		
		obdq = new OBDQuery(obdsql, conn, new String[]{aq, tq, gq});
		characters = new ArrayList<String>();
		qualityToTaxonMap = new HashMap<String, Set<String>>();
		qualityToGenotypeMap = new HashMap<String, Set<String>>();
		qualityToGeneMap = new HashMap<String, Set<String>>();
		characterToQualityMap  = new HashMap<String, Set<String>>();
		qualitiesToCharacterMap  = new HashMap<String, String>();
		annotationCount = 0;
		statementCountByTaxa = new HashMap<String, Integer>();
		statementCountByGenotype = new HashMap<String, Integer>();
		statementCountByGene = new HashMap<String, Integer>();
		jObjs = new JSONObject();
		// System.out.println(termId);
	}

	/**
	 * This is an updated method for the new code that executes faster queries DT:03-16-09
	 */
	public Representation getRepresentation(Variant variant) {

		Representation rep;

		try {
			if (!termId.startsWith("TAO:") && !termId.startsWith("ZFA:")) {
				this.jObjs = null;
				getResponse().setStatus(
						Status.CLIENT_ERROR_BAD_REQUEST,
						"ERROR: The input parameter "
								+ "is not a recognized anatomical entity");
				return null;
			}
			if (obdsql.getNode(this.termId) != null) {
				JSONObject termObject = new JSONObject();
				String term = obdsql.getNode(this.termId).getLabel();
				termObject.put("id", this.termId);
				termObject.put("name", term);
				this.jObjs.put("term", termObject);
				
				Map<String, List<List<Map<String, String>>>> nodesByChar = getNewAnatomyTermSummary(termId);
				
				JSONObject charObj, geneForCharObj, taxonForCharObj, geneAnnotSummaryObj, taxonAnnotSummaryObj;
				List<JSONObject> charObjs = new ArrayList<JSONObject>();
				List<JSONObject> genesForCharObj;
				List<JSONObject> taxaForCharObj;
				for(String cStr : nodesByChar.keySet()){
					List<List<Map<String, String>>> allNodes = nodesByChar.get(cStr);
					genesForCharObj = new ArrayList<JSONObject>();
					taxaForCharObj = new ArrayList<JSONObject>();
					charObj = new JSONObject();
					List<Map<String, String>> gNodes = allNodes.get(0);
					List<Map<String, String>> tNodes = allNodes.get(1);
					charObj.put("character", cStr);
					charObj.put("totalAnnotationsCount", tNodes.size() + gNodes.size());
					geneAnnotSummaryObj = new JSONObject();
					taxonAnnotSummaryObj = new JSONObject();
					geneAnnotSummaryObj.put("annotationCount", gNodes.size());
					geneSet = new HashSet<String>();
					for(Map<String, String> gNode : gNodes){
						geneForCharObj = new JSONObject();
						geneForCharObj.put("gene", gNode.get("exhibitedBy"));
						geneSet.add(gNode.get("exhibitedBy"));
						geneForCharObj.put("state", gNode.get("hasState")); 
						geneForCharObj.put("entity", gNode.get("inheresIn"));
						genesForCharObj.add(geneForCharObj);
					}
					geneAnnotSummaryObj.put("geneCount", geneSet.size());
					taxonAnnotSummaryObj.put("annotationCount", tNodes.size());
					taxonSet = new HashSet<String>();
					for(Map<String, String> tNode : tNodes){
						taxonForCharObj = new JSONObject();
						taxonForCharObj.put("taxon", tNode.get("exhibitedBy"));
						taxonSet.add(tNode.get("exhibitedBy"));
						taxonForCharObj.put("state", tNode.get("hasState"));
						taxonForCharObj.put("entity", tNode.get("inheresIn"));
						taxaForCharObj.add(taxonForCharObj);
					}
					taxonAnnotSummaryObj.put("taxonCount", taxonSet.size());
					//charObj.put("geneAnnotationDetails", genesForCharObj);
					//charObj.put("taxonAnnotationDetails", taxaForCharObj);
					charObj.put("taxonAnnotations", taxonAnnotSummaryObj);
					charObj.put("geneAnnotations", geneAnnotSummaryObj);
					charObjs.add(charObj);
				}
				this.jObjs.put("qualities", charObjs);
			} else {
				getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND,
						"The search term was not found");
				return null;
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		rep = new JsonRepresentation(this.jObjs);
		return rep;
	}

	/**
	 * These new methods are being developed for the ASIH prototype being developed for the
	 * ASIH meeting Portland, OR in July, 2009
	 */
	
	private Map<String, List<List<Map<String, String>>>> getNewAnatomyTermSummary(String termId) throws IOException, SQLException, 
	ClassNotFoundException, JSONException, IllegalArgumentException{
		Map<String, List<List<Map<String, String>>>> nodesByChar = new HashMap<String, List<List<Map<String, String>>>>();
		List<Map<String, String>> nodesByGenes;
		List<Map<String, String>> nodesByTaxa;
		List<List<Map<String, String>>> combinedNodes;
		Map<String, String> nodeProps;
		String relId, target, character = null, taxon = null;
		
		for(Node node : obdq.executeQuery(obdq.getAnatomyQuery(), termId)){
			nodeProps = new HashMap<String, String>();
			for(Statement stmt : node.getStatements()){
				relId = stmt.getRelationId();
				target = stmt.getTargetId();
				nodeProps.put(relId, target);
			} 
			nodeProps.put("id", node.getId());
			character = nodeProps.get("hasCharacter");
			taxon = nodeProps.get("exhibitedBy");
			
			combinedNodes = nodesByChar.containsKey(character)? nodesByChar.get(character) : new ArrayList<List<Map<String, String>>>();	
			nodesByGenes =  (combinedNodes.size() > 0) ? combinedNodes.get(0) : new ArrayList<Map<String, String>>();		
			nodesByTaxa =  (combinedNodes.size() > 0) ? combinedNodes.get(1) : new ArrayList<Map<String, String>>();					
				
			if(taxon.contains("GENE"))
				nodesByGenes.add(nodeProps);
			else
				nodesByTaxa.add(nodeProps);
			combinedNodes.add(nodesByGenes);
			combinedNodes.add(nodesByTaxa);
			
			nodesByChar.put(character, combinedNodes);
		}
		return nodesByChar;
	}
	
	/**
	 * These methods were implemented for the SICB prototype demonstrated at the SICB meeting in Boston, MA
	 * in January 2009
	 */
	
	/**
	 * The method to summarize anatomical terms
	 * @param termId
	 * @throws IOException
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws JSONException
	 * @throws IllegalArgumentException
	 */
	@Deprecated
	private void getAnatomyTermSummary(String termId) throws IOException,
	SQLException, ClassNotFoundException, JSONException,
	IllegalArgumentException {

		// start working with the given anatomical feature
		String nodeId, targetId, patoId;

		Collection<Statement> stmts = obdq.getStatementsWithPredicateAndObject(
											termId, EXHIBITS_RELATION);

		for (Statement stmt : stmts) {
			++annotationCount;
			
			nodeId = stmt.getNodeId();
			targetId = stmt.getTargetId();
			if(parseCompositionalDescription(targetId) != null){ //pull out pato term
				patoId = parseCompositionalDescription(targetId);
				String characterId;
				characterId = qualitiesToCharacterMap.get(patoId) != null? //prior mapping exists in reverse lookup
						qualitiesToCharacterMap.get(patoId): //save on database search
						findAttrib(patoId); //otherwise, look in database
						
				if(!characters.contains(characterId)){
					characters.add(characterId);	//avoid duplicate entries. we need to sort this later, so can't use a set
				}
				qualitiesToCharacterMap.put(patoId, characterId); //a reverse mapping: every state has a unique character
				
				if(characterToQualityMap.get(characterId) != null){ //previous mappings exist from character to state
					qualitiesForCharacter = characterToQualityMap.get(characterId); 
					qualitiesForCharacter.add(patoId); //add the new state to existing list
					characterToQualityMap.put(characterId, qualitiesForCharacter);
				}
				else{
					Set<String> newQualitySet = new HashSet<String>();
					newQualitySet.add(patoId); //create new list of states and map it to characters
					characterToQualityMap.put(characterId, newQualitySet);
				}
				if (nodeId.contains("TTO:")) { // "Taxon exhibits Phenotype"
					updateStatementCount(nodeId, patoId);
					if(qualityToTaxonMap.get(patoId) != null){ //if previous mappings exist from PATO term to Taxa
						taxa = qualityToTaxonMap.get(patoId);
						taxa.add(nodeId);						//add this taxon to list
						qualityToTaxonMap.put(patoId, taxa);						
					}
					else{
						Set<String> newTaxaSet = new HashSet<String>();
						newTaxaSet.add(nodeId);					//create new list for this PATO term
						qualityToTaxonMap.put(patoId, newTaxaSet);
					}

				} else if (nodeId.contains("GENO")) { // "Genotype exhibits Phenotype"
					updateStatementCount(nodeId, patoId);
					if(qualityToGenotypeMap.get(patoId) != null){ //if previos mappings exist from PATO to genotypes
						genotypes = qualityToGenotypeMap.get(patoId);
						genotypes.add(nodeId);	//add this genotype to list
						qualityToGenotypeMap.put(patoId, genotypes);
					}
					else{
						Set<String> newGenotypeSet = new HashSet<String>();
						newGenotypeSet.add(nodeId);		//create new list and add it to PATO term
						qualityToGenotypeMap.put(patoId, newGenotypeSet);
					}
					if(getGeneForGenotype(patoId, nodeId) != null){
						String gene = getGeneForGenotype(patoId, nodeId);
						updateStatementCount(gene, patoId);
						if(qualityToGeneMap.get(patoId) != null){ //if previous mappings exist from PATO term to genes
							genes = qualityToGeneMap.get(patoId);
							genes.add(gene);	//add this gene to the list
							qualityToGeneMap.put(patoId, genes);
						}
						else{
							Set<String> newGeneSet = new HashSet<String>();
							newGeneSet.add(gene);  //create a new list and add it to PATO term
							qualityToGeneMap.put(patoId, newGeneSet);
						}
					}
				}
			}
		}

		// look for subclasses of the input anatomical feature and find "their"
		// 		qualities and genes
		if (obdq.getStatementsWithPredicateAndObject(termId, IS_A_RELATION)
											.size() > 0) {
			for (Statement scStmt : obdq.getStatementsWithPredicateAndObject(
							termId, IS_A_RELATION)) {
				getAnatomyTermSummary(scStmt.getNodeId());
			}
		}
	}
	/**
	 * A method to keep track of statement counts for every taxon, genotype and gene
	 * @param term
	 */
	
	private void updateStatementCount(String term, String pato) {
		String index = term + "\t" + pato;
		if(term.contains("TTO")){
			if(statementCountByTaxa.get(index) != null){
				int ct = statementCountByTaxa.get(index);
				statementCountByTaxa.put(index, ++ct);
			}
			else{
				statementCountByTaxa.put(index, 1);
			}
		}
		else if(term.contains("GENO")){
			if(statementCountByGenotype.get(index) != null){
				int ct = statementCountByGenotype.get(index);
				statementCountByGenotype.put(index, ++ct);
			}
			else{
				statementCountByGenotype.put(index, 1);
			}
		}
		else if(term.contains("GENE")){
			if(statementCountByGene.get(index) != null){     
				int ct = statementCountByGene.get(index);
				statementCountByGene.put(index, ++ct);
			}
			else{
				statementCountByGene.put(index, 1);
			}
		}
	}

	/**
	 * A helper method to find the parent PATO term through the slims hierarchy
	 * @param patoTerm
	 * @return
	 */
	
	private String findAttrib(String patoTerm) {
		String parentId;
		Set<String> subsets = new HashSet<String>();
		Collection<Statement> subsetColl = obdq.getStatementsWithSubjectAndPredicate(patoTerm,
				OBOOWL_SUBSET_RELATION);
		if (subsetColl.size() > 0) {
			for(Statement s : subsetColl){
				subsets.add(s.getTargetId());
			}
			if(subsets.contains(VALUE_SLIM_STRING)){
				Collection<Statement> superPatoTermStatements = 
					obdq.getStatementsWithSubjectAndPredicate(patoTerm, IS_A_RELATION);
				if(superPatoTermStatements.size() > 0){
					
					for(Statement patoStmt : superPatoTermStatements ){
						parentId = patoStmt.getTargetId();
						if(!parentId.contains("^") && !parentId.equals(patoTerm)){
							return findAttrib(parentId);
						}
					}
				}
			}
		}
		return patoTerm.contains("^") ? parseCompositionalDescription(patoTerm)
						: patoTerm;
	}

	/**
	 * A method to extract PATO and Anatomical terms from Compositional Description
	 * @param cd
	 * @return
	 */
	private String parseCompositionalDescription(String cd) {
		String quality = null;
		
		Pattern patoPattern = Pattern.compile("PATO:[0-9]+");
		Matcher patoMatcher = patoPattern.matcher(cd);
		if(patoMatcher.find()){
			quality = cd.substring(patoMatcher.start(), patoMatcher.end());
		}
		return quality;
	}

	/**
	 * A method to find the Gene a Genotype is an allele of
	 * 
	 * @param genotypeId
	 */
	private String getGeneForGenotype(String quality, String genotypeId) {
		Collection<Statement> stmts = obdq.genericTermSearch(genotypeId);
		for (Statement stmt : stmts) {
			if (stmt.getRelationId().equals(HAS_ALLELE_RELATION)) {
				return stmt.getNodeId();
			}
		}
		return null;
	}
}
