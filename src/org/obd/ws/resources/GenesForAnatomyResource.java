package org.obd.ws.resources;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
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

public class GenesForAnatomyResource extends Resource {

	private final String termId;
	private final String patoId;

	private JSONObject jObjs;
	private Shard obdsql;
	private Connection conn;
	private OBDQuery obdq;

	Set<String> subQualities = new HashSet<String>();
	Set<String> subFeatures = new HashSet<String>();

	Set<String> genotypeAnnotations = new HashSet<String>();
	Set<String> geneAnnotations = new HashSet<String>();

	private final String IS_A_RELATION = "OBO_REL:is_a";
	private final String EXHIBITS_RELATION = "PHENOSCAPE:exhibits";
	private final String HAS_ALLELE_RELATION = "PHENOSCAPE:has_allele";
	
	public GenesForAnatomyResource(Context context, Request request,
			Response response) {
		super(context, request, response);

		this.obdsql = (Shard) this.getContext().getAttributes().get("shard");
		this.conn = (Connection)getContext().getAttributes().get("conn");
		this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
		// this.getVariants().add(new Variant(MediaType.TEXT_HTML));
		this.termId = Reference.decode((String) (request.getAttributes()
				.get("termID")));
		this.patoId = Reference.decode((String) (request.getAttributes()
				.get("patoID")));
		obdq = new OBDQuery(obdsql, conn);

		jObjs = new JSONObject();
	}
	
	public Representation getRepresentation(Variant variant) {

		Representation rep;

		try {
			if ((!termId.startsWith("TAO:") && !termId.startsWith("ZFA:"))
					|| (!patoId.startsWith("PATO:"))) {
				this.jObjs = null;
				getResponse().setStatus(
						Status.CLIENT_ERROR_BAD_REQUEST,
						"ERROR: The input parameter "
								+ "is not a recognized anatomical entity");
				return null;
			}
			if (obdsql.getNode(this.termId) != null
					&& obdsql.getNode(this.patoId) != null) {
				JSONObject termObject = new JSONObject();
				String term = obdsql.getNode(this.termId).getLabel();
				termObject.put("id", this.termId);
				termObject.put("name", term);
				this.jObjs.put("anatomical_feature", termObject);
				JSONObject patoObject = new JSONObject();
				String pato = obdsql.getNode(this.patoId).getLabel();
				patoObject.put("id", this.patoId);
				patoObject.put("name", pato);
				this.jObjs.put("quality", patoObject);

				getAnatomyAndQualityTermInfo(this.termId, this.patoId);

				JSONObject genotypeObj, geneObj, genoAnnotObj, geneAnnotObj;
				JSONObject entityObj, qualityObj;

				List<JSONObject> genotypeList = new ArrayList<JSONObject>();
				List<JSONObject> geneList = new ArrayList<JSONObject>();

				if (genotypeAnnotations.size() > 0) {
					for (String gAnnot : genotypeAnnotations) {
						genotypeObj = new JSONObject();
						entityObj = new JSONObject();
						qualityObj = new JSONObject();
						genoAnnotObj = new JSONObject();
						String[] gComps = gAnnot.split("\\t");
						genotypeObj.put("id", gComps[0]);
						genotypeObj.put("name", obdsql.getNode(gComps[0])
								.getLabel());
						entityObj.put("id", gComps[1]);
						entityObj.put("name", obdsql.getNode(gComps[1])
								.getLabel());
						qualityObj.put("id", gComps[2]);
						qualityObj.put("name", obdsql.getNode(gComps[2])
								.getLabel());
						genoAnnotObj.put("genotype", genotypeObj);
						genoAnnotObj.put("entity", entityObj);
						genoAnnotObj.put("quality", qualityObj);
						genotypeList.add(genoAnnotObj);
					}
				}
				if (geneAnnotations.size() > 0) {
					for (String gAnnot : geneAnnotations) {
						geneObj = new JSONObject();
						genotypeObj = new JSONObject();
						entityObj = new JSONObject();
						qualityObj = new JSONObject();
						geneAnnotObj = new JSONObject();
						String[] gComps = gAnnot.split("\\t");
						geneObj.put("id", gComps[0]);
						geneObj.put("name", obdsql.getNode(gComps[0])
								.getLabel());
						genotypeObj.put("id", gComps[1]);
						genotypeObj.put("name", obdsql.getNode(gComps[1])
								.getLabel());
						entityObj.put("id", gComps[2]);
						entityObj.put("name", obdsql.getNode(gComps[2])
								.getLabel());
						qualityObj.put("id", gComps[3]);
						qualityObj.put("name", obdsql.getNode(gComps[3])
								.getLabel());
						geneAnnotObj.put("gene", geneObj);
						geneAnnotObj.put("genotype", genotypeObj);
						geneAnnotObj.put("entity", entityObj);
						geneAnnotObj.put("quality", qualityObj);
						geneList.add(geneAnnotObj);
					}
				}
				this.jObjs.put("other_annotations", genotypeList);
				this.jObjs.put("annotations", geneList);

			} else {
				getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND,
						"The search term was not found");
				return null;
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		rep = new JsonRepresentation(this.jObjs);
		return rep;
	}
	
	private void getAnatomyAndQualityTermInfo(String anatId, String patoId) {
		computeHierarchy(patoId);
		computeHierarchy(anatId);
		List<String> features = new ArrayList<String>();
		List<String> qualities = new ArrayList<String>();
		features.add(anatId);
		features.addAll(subFeatures);
		qualities.add(patoId);
		qualities.addAll(subQualities);
		String taxOrGenoId, phenotypeId, featureId;

		for (String f : features) {
			for (String q : qualities) {
				Collection<Statement> stmts = obdq
						.getStatementsWithPredicateAndObject(q
								+ "%inheres%" + f, EXHIBITS_RELATION);
				for (Statement stmt : stmts) {
					taxOrGenoId = stmt.getNodeId();
					phenotypeId = stmt.getTargetId();

					if (parseCompositionalDescription(phenotypeId) != null) {
						featureId = parseCompositionalDescription(phenotypeId).split("\\t")[1];
						if (taxOrGenoId.contains("GENO")) {
							if (getGeneForGenotype(taxOrGenoId) != null) {
								String gene = getGeneForGenotype(taxOrGenoId);
									geneAnnotations.add(gene + "\t" + taxOrGenoId + "\t"
										+ featureId + "\t" + q);
							}
							else{
								genotypeAnnotations.add(taxOrGenoId + "\t"
										+ featureId + "\t" + q);
							}
						}
					}
				}
			}
		}
	}
	
	private String getGeneForGenotype(String taxOrGenoId) {
		Collection<Statement> stmts = obdq.genericTermSearch(taxOrGenoId);
		for (Statement stmt : stmts) {
			if (stmt.getRelationId().equals(HAS_ALLELE_RELATION)) {
				return stmt.getNodeId();
			}
		}
		return null;
	}


	private void computeHierarchy(String term) {
		Collection<Statement> stmts = obdq.getStatementsWithPredicateAndObject(
				term, IS_A_RELATION);
		if (stmts.size() > 0) {
			for (Statement stmt : stmts) {
				if (!stmt.getNodeId().equals(term)
						&& !stmt.getNodeId().contains(term)){
					if (term.contains("TAO") || term.contains("ZFA")) {
						subFeatures.add(stmt.getNodeId());
						computeHierarchy(stmt.getNodeId());
					} else if (term.contains("PATO")) {
						subQualities.add(stmt.getNodeId());
						computeHierarchy(stmt.getNodeId());
					}
				}
			}
		}
		return;
	}
	
	private String parseCompositionalDescription(String cd) {
		String quality = null, entity = null;

		Pattern patoPattern = Pattern.compile("PATO:[0-9]+");
		Matcher patoMatcher = patoPattern.matcher(cd);
		if (patoMatcher.find()) {
			quality = cd.substring(patoMatcher.start(), patoMatcher.end());
		}
		Pattern anatPattern = Pattern.compile("(ZFA|TAO):\\d+");
		Matcher anatMatcher = anatPattern.matcher(cd);
		if (anatMatcher.find()) {
			entity = cd.substring(anatMatcher.start(), anatMatcher.end());
		}
		return (quality != null && entity != null) ? quality + "\t" + entity
				: null;
	}
}
