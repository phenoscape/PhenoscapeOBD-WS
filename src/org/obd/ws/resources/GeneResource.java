package org.obd.ws.resources;

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

public class GeneResource extends Resource {

	private final String termId;
	private JSONObject jObjs;
	private Shard obdsql;
	private OBDQuery obdq;
	
	private Set<String> annotations;
	
	private final String IS_A_RELATION = "OBO_REL:is_a";
	private final String EXHIBITS_RELATION = "PHENOSCAPE:exhibits";
	private final String HAS_ALLELE_RELATION = "PHENOSCAPE:has_allele";
	
	
	public GeneResource(Context context, Request request, Response response){
		super(context, request, response);
		
		obdsql = (Shard)getContext().getAttributes().get("shard");
		termId = Reference.decode((String) (request.getAttributes()
				.get("termID")));
		obdq = new OBDQuery(obdsql);
		jObjs = new JSONObject();
		
		annotations = new HashSet<String>();
	
		getVariants().add(new Variant(MediaType.APPLICATION_JSON));
	}
	
	public Representation getRepresentation(Variant variant){
		Representation rep;
		
		if(!termId.contains("GENE") && !termId.contains("GENO")){
			jObjs = null;
			getResponse().setStatus(
					Status.CLIENT_ERROR_BAD_REQUEST,
					"ERROR: The input parameter "
							+ "is not a recognized gene or genotype");
			return null;
		}
		if(obdsql.getNode(termId) != null){
			try{
				JSONObject genObject = new JSONObject();
				genObject.put("id", termId);
				genObject.put("name", obdsql.getNode(termId).getLabel());
				jObjs.put("term", genObject);
				if(termId.contains("GENO")){
					getGenotypeSummary(termId);
				}
				else{
					getGeneSummary(termId);
				}
				
				if(annotations.size() > 0){
					JSONObject annotationObj, featureObj, qualityObj;
					List<JSONObject> annotationObjList = new ArrayList<JSONObject>();
					for(String annotation : annotations){
						annotationObj = new JSONObject();
						featureObj  = new JSONObject();
						qualityObj  = new JSONObject();
						String[] aComps = annotation.split("\\t");
						featureObj.put("id", aComps[0]);
						featureObj.put("name", obdsql.getNode(aComps[0]).getLabel());
						qualityObj.put("id", aComps[1]);
						qualityObj.put("name", obdsql.getNode(aComps[1]).getLabel());
						annotationObj.put("entity", featureObj);
						annotationObj.put("quality", qualityObj);
						annotationObjList.add(annotationObj);
					}
					jObjs.put("annotations", annotationObjList);
				}
				else{
					jObjs.put("annotations", "[]");
				}
			}
			catch(JSONException jsone){
				jsone.printStackTrace();
			}
			
		}
		else{
			getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND,
										"The search term was not found");
			return null;
		}
		rep = new JsonRepresentation(jObjs);
		return rep;
	}

	private void getGeneSummary(String geneId) {
		Collection<Statement> stmts = obdq.getStatementsWithSubjectAndPredicate(geneId, HAS_ALLELE_RELATION);
		for(Statement stmt : stmts){
			String genotypeId = stmt.getTargetId();
			getGenotypeSummary(genotypeId);
		}
	}

	private void getGenotypeSummary(String genotypeId) {
		String phenotypeId, qualityId, featureId;

		Collection<Statement> stmts = obdq.getStatementsWithSubjectAndPredicate(genotypeId, EXHIBITS_RELATION);
		for(Statement stmt : stmts){
			phenotypeId = stmt.getTargetId();
			if(parseCompositionalDescription(phenotypeId) != null){
				String[] comps = parseCompositionalDescription(phenotypeId).split("\\t");
				qualityId = comps[0]; 
				featureId = comps[1];
				annotations.add(featureId + "\t" + qualityId);
				if(getTeleostEquivalent(featureId) != null){
					annotations.add(getTeleostEquivalent(featureId) + "\t" + qualityId);
				}
			}
		}
	}

	/**
	 * This method finds the TAO equivalent of a ZFA feature, if one exists
	 * @param featureId
	 * @return
	 */
	private String getTeleostEquivalent(String featureId) {
		
		Collection<Statement> stmts = obdq.getStatementsWithSubjectAndPredicate(featureId, IS_A_RELATION);
		for(Statement stmt : stmts){
			String taoTerm = stmt.getTargetId();
			if(taoTerm.startsWith("TAO")){
				if(taoTerm.substring(taoTerm.indexOf(":")).equals(featureId.substring(featureId.indexOf(":")))){
					return taoTerm;
				}
			}
		}
		return null;
	}

	/**
	 * This method parses the compositional description to return a tab delimited combination
	 * of entity and quality (or null)
	 * @param cd
	 * @return
	 */
	private String parseCompositionalDescription(String cd) {
		String quality = null, entity = null;
		
		Pattern patoPattern = Pattern.compile("PATO:[0-9]+");
		Matcher patoMatcher = patoPattern.matcher(cd);
		if(patoMatcher.find()){
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
