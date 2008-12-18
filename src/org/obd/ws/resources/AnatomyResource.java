package org.obd.ws.resources;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONException;
import org.json.JSONObject;
import org.nescent.informatics.OBDQuery;
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
	private OBDQuery obdq;
	private Set<String> qualities;
	private Set<String> taxa;
	private Set<String> genotypes;
	private Set<String> genes;
	
	private Map<String, Set<String>> qualityToTaxonMap;
	private Map<String, Set<String>> qualityToGenotypeMap;
	private Map<String, Set<String>> qualityToGeneMap;

	private int annotationCount;

	private final String OBOOWL_SUBSET_RELATION = "oboInOwl:inSubset";
	private final String IS_A_RELATION = "OBO_REL:is_a";
	private final String EXHIBITS_RELATION = "PHENOSCAPE:exhibits";
	private final String HAS_ALLELE_RELATION = "PHENOSCAPE:has_allele";

	private final String VALUE_SLIM_STRING = "value_slim";
	
	public AnatomyResource(Context context, Request request, Response response) {
		super(context, request, response);

		this.obdsql = (Shard) this.getContext().getAttributes().get("shard");
		this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
		// this.getVariants().add(new Variant(MediaType.TEXT_HTML));
		this.termId = Reference.decode((String) (request.getAttributes()
				.get("termID")));
		obdq = new OBDQuery(obdsql);
		qualities = new HashSet<String>();
		taxa = new HashSet<String>();
		genotypes = new HashSet<String>();
		genes = new HashSet<String>();
		qualityToTaxonMap = new HashMap<String, Set<String>>();
		qualityToGenotypeMap = new HashMap<String, Set<String>>();
		qualityToGeneMap = new HashMap<String, Set<String>>();
		annotationCount = 0;
		jObjs = new JSONObject();
		// System.out.println(termId);
	}

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
				getAnatomyTermSummary(this.termId);
				if(qualities.size() > 0){
					Set<JSONObject> qualityObjs = new HashSet<JSONObject>();
					for(String patoStr : qualities){
						JSONObject qualityObj = new JSONObject();
						String character = obdsql.getNode(findAttrib(patoStr)).getLabel();
						String state = obdsql.getNode(patoStr).getLabel();
						qualityObj.put("id", patoStr);
						qualityObj.put("name", (character.equals(state)? state : character.toUpperCase(Locale.US) + ": " + state));
						JSONObject taxonObj = new JSONObject();
						JSONObject genotypeObj = new JSONObject();
						JSONObject geneObj = new JSONObject();
						if(qualityToTaxonMap.get(patoStr) != null){
							taxonObj.put("annotation_count", annotationCount);
							taxonObj.put("taxon_count", qualityToTaxonMap.get(patoStr).size());
							qualityObj.put("taxon_annotations", taxonObj);
						}
						if(qualityToGenotypeMap.get(patoStr) != null){
							genotypeObj.put("annotation_count", annotationCount);
							genotypeObj.put("genotype_count", qualityToGenotypeMap.get(patoStr).size());
							qualityObj.put("genotype_annotations", genotypeObj);
						}
						if(qualityToGeneMap.get(patoStr) != null){
							geneObj.put("annotation_count", annotationCount);
							geneObj.put("gene_count", qualityToGeneMap.get(patoStr).size());
							qualityObj.put("gene_annotations", geneObj);
						}
						qualityObjs.add(qualityObj);
					}
					this.jObjs.put("qualities", qualityObjs);
				}
				else{	
					this.jObjs.put("qualities", "[]");
				}
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
	 * The method to summarize anatomical terms
	 * @param termId
	 * @throws IOException
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws JSONException
	 * @throws IllegalArgumentException
	 */
	private void getAnatomyTermSummary(String termId) throws IOException,
	SQLException, ClassNotFoundException, JSONException,
	IllegalArgumentException {

		// start working with the given anatomical feature
		String nodeId, targetId, patoTerm;

		Collection<Statement> stmts = obdq.getStatementsWithPredicateAndObject(
											termId, EXHIBITS_RELATION);

		for (Statement stmt : stmts) {
			++annotationCount;
			
			nodeId = stmt.getNodeId();
			targetId = stmt.getTargetId();
			if(parseCompositionalDescription(targetId) != null){ //pull out pato term
				patoTerm = parseCompositionalDescription(targetId);
				qualities.add(patoTerm);
				if (nodeId.contains("TTO:")) { // "Taxon exhibits Phenotype"
					taxa.add(nodeId);
					qualityToTaxonMap.put(patoTerm, taxa);
				} else if (nodeId.contains("GENO")) { // "Genotype exhibits Phenotype"
					genotypes.add(nodeId);
					qualityToGenotypeMap.put(patoTerm, genotypes);
					if(getGeneForGenotype(patoTerm, nodeId) != null){
						String gene = getGeneForGenotype(patoTerm, nodeId);
						genes.add(gene);
						qualityToGeneMap.put(patoTerm, genes);
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
	 * A helper method to find the parent PATO term through the slims hierarchy
	 * @param patoTerm
	 * @return
	 */
	
	private String findAttrib(String patoTerm) {
		String parentId, valOrAttrib;
		if(obdq.getStatementsWithSubjectAndPredicate(patoTerm, OBOOWL_SUBSET_RELATION).size() > 0){
			valOrAttrib = obdq.getStatementsWithSubjectAndPredicate(patoTerm, OBOOWL_SUBSET_RELATION).
								iterator().next().getTargetId();
			if(valOrAttrib.equals(VALUE_SLIM_STRING)){
				for(Statement s : obdq.getStatementsWithSubjectAndPredicate(patoTerm, IS_A_RELATION)){
					parentId = s.getTargetId();
					if(!parentId.equals(patoTerm)){
						return findAttrib(parentId);
					}
				}
			}
		}
		return patoTerm;
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
