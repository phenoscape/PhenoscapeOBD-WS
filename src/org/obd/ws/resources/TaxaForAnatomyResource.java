package org.obd.ws.resources;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

public class TaxaForAnatomyResource extends Resource {

	private final String termId;
	private final String patoId;
	private String attributeOption = "false";

	private JSONObject jObjs;
	private Shard obdsql;
	private OBDQuery obdq;

	Set<String> subQualities = new HashSet<String>();
	Set<String> subEntities = new HashSet<String>();

	Set<String> taxonAnnotations = new HashSet<String>();

	private final String IS_A_RELATION = "OBO_REL:is_a";
	private final String EXHIBITS_RELATION = "PHENOSCAPE:exhibits";
	private final String OBOOWL_SUBSET_RELATION = "oboInOwl:inSubset";

	private final String VALUE_SLIM_STRING = "value_slim";

	public TaxaForAnatomyResource(Context context, Request request,
			Response response) {
		super(context, request, response);

		obdsql = (Shard) getContext().getAttributes().get("shard");
		getVariants().add(new Variant(MediaType.APPLICATION_JSON));
		// this.getVariants().add(new Variant(MediaType.TEXT_HTML));
		termId = Reference.decode((String) (request.getAttributes()
				.get("termID")));
		patoId = Reference.decode((String) (request.getAttributes()
				.get("patoID")));
		if (request.getResourceRef().getQueryAsForm()
				.getFirstValue("attribute") != null)
			attributeOption = Reference.decode((String) request
					.getResourceRef().getQueryAsForm().getFirstValue(
							"attribute"));
		obdq = new OBDQuery(obdsql);

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

			if (obdsql.getNode(termId) != null
					&& obdsql.getNode(patoId) != null) {
				JSONObject termObject = new JSONObject();
				String term = obdsql.getNode(termId).getLabel();
				termObject.put("id", termId);
				termObject.put("name", term);
				this.jObjs.put("entity", termObject);
				JSONObject patoObject = new JSONObject();
				String pato = obdsql.getNode(patoId).getLabel();
				patoObject.put("id", patoId);
				patoObject.put("name", pato);
				this.jObjs.put("quality", patoObject);

				getAnatomyAndQualityTermInfo(termId, patoId, attributeOption);

				JSONObject taxonObj, entityObj, qualityObj, taxonAnnotObj;

				List<JSONObject> taxonList = new ArrayList<JSONObject>();

				if (taxonAnnotations.size() > 0) {
					for (String tAnnot : taxonAnnotations) {
						taxonObj = new JSONObject();
						entityObj = new JSONObject();
						qualityObj = new JSONObject();
						taxonAnnotObj = new JSONObject();
						String[] tComps = tAnnot.split("\\t");
						taxonObj.put("id", tComps[0]);
						taxonObj.put("name", obdsql.getNode(tComps[0])
								.getLabel());
						entityObj.put("id", tComps[1]);
						entityObj.put("name", obdsql.getNode(tComps[1])
								.getLabel());
						qualityObj.put("id", tComps[2]);
						qualityObj.put("name", obdsql.getNode(tComps[2])
								.getLabel());
						taxonAnnotObj.put("taxon", taxonObj);
						taxonAnnotObj.put("entity", entityObj);
						taxonAnnotObj.put("quality", qualityObj);
						taxonList.add(taxonAnnotObj);
					}
				}
				this.jObjs.put("annotations", taxonList);

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

	private void getAnatomyAndQualityTermInfo(String anatId, String patoId,
			String attributeOption) {

		if (attributeOption.equals("true")) {
			Collection<Statement> subsetStmts = obdq
					.getStatementsWithSubjectAndPredicate(patoId,
							OBOOWL_SUBSET_RELATION);
			Set<String> subsets = new HashSet<String>();
			if (subsetStmts.size() > 0) {
				for (Statement subsetStmt : subsetStmts) {
					subsets.add(subsetStmt.getTargetId());
				}
				if (subsets.contains(VALUE_SLIM_STRING)) {
					findAncestors(patoId);
				}
			}
		}
		findDescendants(patoId);
		findDescendants(anatId);

//		System.out.println("computed hierarchies");
		List<String> features = new ArrayList<String>();
		List<String> qualities = new ArrayList<String>();
		features.add(anatId);
		features.addAll(subEntities);
		qualities.add(patoId);
		qualities.addAll(subQualities);

		String taxOrGenoId;

		for (String f : features) {
			for (String q : qualities) {
				Collection<Statement> stmts = obdq
						.getStatementsWithPredicateAndObject(q + "%inheres%"
								+ f, EXHIBITS_RELATION);
				for (Statement stmt : stmts) {
					taxOrGenoId = stmt.getNodeId();
					if (taxOrGenoId.contains("TTO")) {
						taxonAnnotations.add(taxOrGenoId + "\t" + f + "\t" + q);
					}
				}
			}
		}
	}

	private void findAncestors(String patoId2) {
		Collection<Statement> parentStmts = obdq
				.getStatementsWithSubjectAndPredicate(patoId2, IS_A_RELATION);
		Set<String> slims = new HashSet<String>();
		if (parentStmts != null & parentStmts.size() > 0) {
			for (Statement s : parentStmts) {
				String parentId = s.getTargetId();
				subQualities.add(parentId);
				Collection<Statement> slimStmts = obdq
						.getStatementsWithSubjectAndPredicate(parentId,
								OBOOWL_SUBSET_RELATION);
				for (Statement slimStmt : slimStmts) {
					slims.add(slimStmt.getTargetId());
				}
				if (slims.contains(VALUE_SLIM_STRING)) {
					findAncestors(parentId);
				}
				findDescendants(parentId);
			}
		}
	}

	private void findDescendants(String term) {
		if (!subEntities.contains(term) && !subQualities.contains(term)) {
			if(term.startsWith("TAO") || term.startsWith("ZFA")){
				subEntities.add(term);
				//System.out.println("Adding entity: " + term);
			}
			else if(term.startsWith("PATO")){
				subQualities.add(term);
				//System.out.println("Adding quality: " + term);
			}
			Collection<Statement> stmts = obdq
					.getStatementsWithPredicateAndObject(term, IS_A_RELATION);
			if (stmts.size() > 0) {
				for (Statement stmt : stmts) {
					if (!stmt.getNodeId().equals(term)
							&& !stmt.getNodeId().contains(term)) {
							findDescendants(stmt.getNodeId());
					}
				}
			}
		}
		return;
	}
}
