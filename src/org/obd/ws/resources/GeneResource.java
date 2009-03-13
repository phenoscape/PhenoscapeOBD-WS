package org.obd.ws.resources;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
	private Connection conn;
	private OBDQuery obdq;
	
	private Map<String, List<String>> phenotypeToGenotypesMap;
	
	private final String IS_A_RELATION = "OBO_REL:is_a";
	private final String EXHIBITS_RELATION = "PHENOSCAPE:exhibits";
	private final String HAS_ALLELE_RELATION = "PHENOSCAPE:has_allele";
	
	
	public GeneResource(Context context, Request request, Response response){
		super(context, request, response);
		
		obdsql = (Shard)getContext().getAttributes().get("shard");
		conn = (Connection)getContext().getAttributes().get("conn");
		termId = Reference.decode((String) (request.getAttributes()
				.get("termID")));
		obdq = new OBDQuery(obdsql, conn);
		jObjs = new JSONObject();
		
		phenotypeToGenotypesMap = new HashMap<String, List<String>>();
		
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
				jObjs.put("gene", genObject);
				getGeneSummary(termId);
				
				if(phenotypeToGenotypesMap.size() > 0){
					JSONObject annotationObj, featureObj, qualityObj, taoObject, genotypeObj;
					List<JSONObject> annotationObjList = new ArrayList<JSONObject>();
					List<String> genotypes = new ArrayList<String>();
					List<JSONObject> genotypeObjList = new ArrayList<JSONObject>();
					for(String key : phenotypeToGenotypesMap.keySet()){
						annotationObj = new JSONObject();
						featureObj  = new JSONObject();
						qualityObj  = new JSONObject();
						genotypeObjList = new ArrayList<JSONObject>();
						String[] keyComps = key.split("\\t");
						qualityObj.put("id", keyComps[0]);
						qualityObj.put("name", obdsql.getNode(keyComps[0]).getLabel());
						featureObj.put("id", keyComps[1]);
						featureObj.put("name", obdsql.getNode(keyComps[1]).getLabel());
						annotationObj.put("entity", featureObj);
						annotationObj.put("quality", qualityObj);
						if(keyComps.length > 2){
							taoObject = new JSONObject();
							taoObject.put("id", keyComps[2]);
							taoObject.put("name", obdsql.getNode(keyComps[2]).getLabel());
							annotationObj.put("teleost_entity", taoObject);
						}
						genotypes = phenotypeToGenotypesMap.get(key);
						for(String genotypeId : genotypes){
							genotypeObj = new JSONObject();
							genotypeObj.put("id", genotypeId);
							genotypeObj.put("name", obdsql.getNode(genotypeId).getLabel());
							genotypeObjList.add(genotypeObj);
						}
						annotationObj.put("genotypes", genotypeObjList);
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
		String phenotypeId, qualityId, featureId, taoFeatureId, key;

		Collection<Statement> stmts = obdq.getStatementsWithSubjectAndPredicate(genotypeId, EXHIBITS_RELATION);
		for(Statement stmt : stmts){
			phenotypeId = stmt.getTargetId();
			if(parseCompositionalDescription(phenotypeId) != null){
				String[] comps = parseCompositionalDescription(phenotypeId).split("\\t");
				qualityId = comps[0]; 
				featureId = comps[1];
				key = qualityId + "\t" + featureId;
				if(getTeleostEquivalent(featureId) != null){
					taoFeatureId = getTeleostEquivalent(featureId);
					key += "\t" + taoFeatureId;
				}
				
				List<String> genotypes = phenotypeToGenotypesMap.get(key);
				if(genotypes != null){
					genotypes.add(genotypeId);
				}
				else{
					genotypes = new ArrayList<String>();
					genotypes.add(genotypeId);
				}
				phenotypeToGenotypesMap.put(key, genotypes);				
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
