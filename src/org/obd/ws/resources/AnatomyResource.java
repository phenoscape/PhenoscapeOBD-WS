package org.obd.ws.resources;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
	private Set<String> taxa;
	private Set<String> genes;
	private Set<String> genotypes;

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
		taxa = new HashSet<String>();
		genes = new HashSet<String>();
		genotypes = new HashSet<String>();
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
				Set<JSONObject> qualityObjs = new HashSet<JSONObject>();
				if (qualityToTaxonMap != null) {
					for (String quality : qualityToTaxonMap.keySet()) {
						JSONObject qualityTaxonObj = new JSONObject();
						String[] eq = parseCompositionalDescription(quality).split("\\t");
						String character = obdsql.getNode(findAttrib(eq[1])).getLabel();
						//String entity = obdsql.getNode(eq[0]).getLabel();
						String state = obdsql.getNode(eq[1]).getLabel();
						if(state.equals(character)){
							qualityTaxonObj.put("id", eq[1]);
							qualityTaxonObj.put("name", state);
						}
						else{
							qualityTaxonObj.put("id", eq[1]);
							qualityTaxonObj.put("name", character + " " + state);
						}
						qualityTaxonObj.put("annotation_count", annotationCount);
						qualityTaxonObj.put("taxon_count", qualityToTaxonMap.get(quality).size());
						qualityObjs.add(qualityTaxonObj);
					}
				}

				if (qualityToGenotypeMap != null) {
					for (String quality : qualityToGenotypeMap.keySet()) {
						JSONObject qualityGenotypeObj = new JSONObject();
						String[] eq = parseCompositionalDescription(quality).split("\\t");
						
						String character = obdsql.getNode(findAttrib(eq[1])).getLabel();
						//String entity = obdsql.getNode(eq[0]).getLabel().toUpperCase();
						String state = obdsql.getNode(eq[1]).getLabel();
						if(state.equals(character)){
							qualityGenotypeObj.put("id", eq[1]);
							qualityGenotypeObj.put("name", state);
						}
						else{
							qualityGenotypeObj.put("id", eq[1]);
							qualityGenotypeObj.put("name", character + " " + state);
						}
						qualityGenotypeObj.put("annotation_count", annotationCount);
						qualityGenotypeObj.put("genotype_count", qualityToGenotypeMap.get(quality).size());
						qualityObjs.add(qualityGenotypeObj);
					}
				}

				if (qualityToGeneMap != null) {
					for (String quality : qualityToGeneMap.keySet()) {
						JSONObject qualityGeneObj = new JSONObject();
						String[] eq = parseCompositionalDescription(quality).split("\\t");
						
						String character = obdsql.getNode(findAttrib(eq[1])).getLabel();
						//String entity = obdsql.getNode(eq[0]).getLabel();
						String state = obdsql.getNode(eq[1]).getLabel();
						if(state.equals(character)){
							qualityGeneObj.put("id", eq[1]);
							qualityGeneObj.put("name", state);
						}
						else{
							qualityGeneObj.put("id", eq[1]);
							qualityGeneObj.put("name", character + " " + state);
						}
						qualityGeneObj.put("annotation_count", annotationCount);
						qualityGeneObj.put("gene_count", qualityToGeneMap.get(quality).size());
						qualityObjs.add(qualityGeneObj);
					}
				}
				this.jObjs.put("qualities", qualityObjs);
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
		String nodeId, targetId;

		Collection<Statement> stmts = obdq.getStatementsWithPredicateAndObject(
											termId, EXHIBITS_RELATION);

		for (Statement stmt : stmts) {
			nodeId = stmt.getNodeId();
			targetId = stmt.getTargetId();
			++annotationCount;

			if (nodeId.contains("TTO:")) { // "Taxon exhibits Phenotype"
				taxa.add(nodeId);
				qualityToTaxonMap.put(targetId, taxa);

			} else if (nodeId.contains("GENO")) { // "Genotype exhibits Phenotype"
				genotypes.add(nodeId);
				qualityToGenotypeMap.put(targetId, genotypes);
				getGeneForGenotype(targetId, nodeId);
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
		valOrAttrib = obdq.getStatementsWithSubjectAndPredicate(patoTerm, OBOOWL_SUBSET_RELATION).iterator().next().getTargetId();
		parentId = obdq.getStatementsWithSubjectAndPredicate(patoTerm, IS_A_RELATION).iterator().next().getTargetId();
		if(parentId != null && !parentId.equals(patoTerm)){
			if(valOrAttrib != null){
				if(valOrAttrib.equals(VALUE_SLIM_STRING)){
					return findAttrib(parentId);
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
		String quality = "", entity = "";
		
		Pattern patoPattern = Pattern.compile("PATO:[0-9]+");
		Matcher patoMatcher = patoPattern.matcher(cd);
		Pattern anatPattern = Pattern.compile("((ZFA)|(TAO)):[0-9]+");
		Matcher anatMatcher = anatPattern.matcher(cd);
		if(patoMatcher.find()){
			quality = cd.substring(patoMatcher.start(), patoMatcher.end());
		}
		if(anatMatcher.find()){
			entity = cd.substring(anatMatcher.start(), anatMatcher.end());
		}
		return entity + "\t" + quality;
	}

	/**
	 * A method to find the Gene a Genotype is an allele of
	 * 
	 * @param genotypeId
	 */
	private void getGeneForGenotype(String quality, String genotypeId) {
		Collection<Statement> stmts = obdq.genericTermSearch(genotypeId);
		for (Statement stmt : stmts) {
			if (stmt.getRelationId().equals(HAS_ALLELE_RELATION)) {
				genes.add(stmt.getNodeId());
			}
		}
		qualityToGeneMap.put(quality, genes);
	}
}
