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
import org.obd.model.LiteralStatement;
import org.obd.model.Node;
import org.obd.model.Statement;
import org.obd.query.LinkQueryTerm;
import org.obd.query.Shard;
import org.obd.query.impl.OBDSQLShard;
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
import org.restlet.resource.ResourceException;

public class TermResource extends Resource {

	private final String termId;
	private JSONObject jObjs;
	private Shard obdsql;
	
	private Map<String, Set<String>> nameToOntologyMap;
	
	public TermResource(Context context, Request request, Response response) {
		super(context, request, response);
		this.obdsql = (Shard) this.getContext().getAttributes().get("shard");
		this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
		// this.getVariants().add(new Variant(MediaType.TEXT_HTML));
		
		Set<String> ontologyList;
		nameToOntologyMap = new HashMap<String, Set<String>>();
		ontologyList = new HashSet<String>();
		ontologyList.add("oboInOwl");
		ontologyList.add("oboInOwl:Subset");
		nameToOntologyMap.put("oboInOwl", ontologyList);
		
		ontologyList = new HashSet<String>();
		ontologyList.add("relationship");
		nameToOntologyMap.put("OBO_REL", ontologyList);

		ontologyList = new HashSet<String>();
		ontologyList.add("quality");
		ontologyList.add("pato.ontology");
		nameToOntologyMap.put("PATO", ontologyList);
		
		ontologyList = new HashSet<String>();
		ontologyList.add("zebrafish_anatomy");
		nameToOntologyMap.put("ZFA", ontologyList);
		
		ontologyList = new HashSet<String>();		
		ontologyList.add("teleost_anatomy");
		nameToOntologyMap.put("TAO", ontologyList);
		
		ontologyList = new HashSet<String>();
		ontologyList.add("teleost-taxonomy");
		nameToOntologyMap.put("TTO",ontologyList);
		
		ontologyList = new HashSet<String>();
		ontologyList.add("museum");
		nameToOntologyMap.put("COLLECTION", ontologyList);
		
		ontologyList = new HashSet<String>();
		ontologyList.add("spatial"); 
		nameToOntologyMap.put("BSPO", ontologyList);
		
		ontologyList = new HashSet<String>();
		ontologyList.add("sequence");
		nameToOntologyMap.put("SO", ontologyList);
		
		ontologyList = new HashSet<String>();
		ontologyList.add("unit.ontology");
		nameToOntologyMap.put("UO", ontologyList);
		
		ontologyList = new HashSet<String>();
		ontologyList.add("phenoscape_vocab");
		nameToOntologyMap.put("PHENOSCAPE", ontologyList);
		this.termId = Reference.decode((String) (request.getAttributes()
				.get("termID")));
	}

	// this constructor is to be used only for testing purposes
	@Deprecated
	public TermResource(Shard obdsql, String termId) {
		this.termId = termId;
		this.obdsql = obdsql;
		this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
	}

        @Override
	public Representation represent(Variant variant) 
            throws ResourceException {

		Representation rep = null;

		try {
			this.jObjs = getTermInfo(this.termId);
			if (this.jObjs == null) {
				getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND,
						"The search term was not found");
				return null;
			}
		} catch (JSONException e) {
                    /* FIXME Never swallow exceptions. Need to rethrow
                     * this to provide information to the client, and
                     * add an appropriate message but that requires
                     * method signature changes.
                     */
                    /* FIXME need to use a logger here */
			e.printStackTrace();
                        throw new ResourceException(e);
		}
		return new JsonRepresentation(this.jObjs);

	}

	private JSONObject getTermInfo(String termId) throws JSONException {

		JSONObject jsonObj = new JSONObject();
		if(termId.indexOf(":") < 0){
			return null;
		}
		if (obdsql.getNode(termId) != null) {
			
			String prefix = termId.substring(0, termId.indexOf(":"));
			
			Set<Statement> sourceStatements = new HashSet<Statement>();
			Set<Statement> targetStatements = new HashSet<Statement>();
			
			LinkQueryTerm slqt, tlqt;
			slqt = new LinkQueryTerm();
			slqt.setNode(termId);
			slqt.setInferred(false);
			
			//this is hacky but it's done because restricting by ontology using QueryTerm.setSource() does not seem to work
			for(Statement stmt : ((OBDSQLShard)obdsql).getLinkStatementsByQuery(slqt)){
				if(!stmt.getTargetId().contains("^") && !stmt.getTargetId().contains("INTERSECTION")
						&& stmt.getTargetId().contains(prefix) && !stmt.getTargetId().contains("RESTRICTION")){
					sourceStatements.add(stmt);
				}
			}
			tlqt = new LinkQueryTerm();
			tlqt.setTarget(termId);
			tlqt.setInferred(false);
			for(Statement stmt : ((OBDSQLShard)obdsql).getLinkStatementsByQuery(tlqt)){
				if(!stmt.getNodeId().contains("^") && !stmt.getNodeId().contains("INTERSECTION") && 
						stmt.getNodeId().contains(prefix) && !stmt.getNodeId().contains("RESTRICTION"))
					targetStatements.add(stmt);
			}
			
			Set<JSONObject> parents = new HashSet<JSONObject>();
			Set<JSONObject> children = new HashSet<JSONObject>();
			Set<JSONObject> synonyms = new HashSet<JSONObject>();
			
			jsonObj.put("id", termId);
			String def = "";
			Collection<LiteralStatement> lstmts = obdsql
					.getLiteralStatementsByNode(termId);
			for (LiteralStatement lstmt : lstmts) {
				if (lstmt.getRelationId().toLowerCase().contains("definition")) {
					def = lstmt.getTargetId();
				}
			}
			String name = obdsql.getNode(termId).getLabel() != null?
							obdsql.getNode(termId).getLabel() : resolveLabel(termId);
			
																		
			jsonObj.put("name", name);
			Collection<Node> synonymNodes = obdsql.getSynonymsForTerm(name);
			String synonym = "No name";
			for(Node node : synonymNodes){
				for(Statement stmt :	node.getStatements()){
					if(stmt.getRelationId().equals("hasSynonym")){
						synonym = stmt.getTargetId();
						JSONObject synonymObj = new JSONObject();
						synonymObj.put("name", synonym);
						synonyms.add(synonymObj);
					}
				}
			}
			jsonObj.put("synonyms", synonyms);
			if (def.length() > 0)
				jsonObj.put("definition", def);
			for (Statement stmt : sourceStatements) {
				String pred = stmt.getRelationId();
				String obj = stmt.getTargetId();
				if (pred != null && pred.length() > 0 &&
						obdsql.getNode(pred) != null && obdsql.getNode(obj) != null) {
						JSONObject parent = new JSONObject();
						JSONObject relation = new JSONObject();
						JSONObject target = new JSONObject();
						relation.put("id", pred);
						relation.put("name", obdsql.getNode(pred).getLabel());
						target.put("id", obj);
						target.put("name", obdsql.getNode(obj).getLabel());
						parent.put("relation", relation);
						parent.put("target", target);
						parents.add(parent);
				}
			}
			for(Statement tStmt : targetStatements){
				String subj = tStmt.getNodeId();
				String pred = tStmt.getRelationId();
				if (pred != null && pred.length() > 0 &&
						obdsql.getNode(pred) != null && obdsql.getNode(subj) != null) {
						JSONObject child = new JSONObject();
						JSONObject relation = new JSONObject();
						JSONObject target = new JSONObject();
						relation.put("id", pred);
						relation.put("name", obdsql.getNode(pred).getLabel());
						target.put("id", subj);
						target.put("name", obdsql.getNode(subj).getLabel());
						child.put("relation", relation);
						child.put("target", target);
						children.add(child);
				}
			}
			jsonObj.put("parents", parents);
			jsonObj.put("children", children);
			// jsonObj.put("otherRelations", otherRelations);
		} else {
			jsonObj = null;
		}
		return jsonObj;
	}
	
    
	public String resolveLabel(String cd){
		String label = cd;
		label = label.replaceAll("\\^", " ");
		String oldLabel = label;
		Pattern pat = Pattern.compile("[A-Z]+_?[A-Z]*:[0-9a-zA-Z]+_?[0-9a-zA-Z]*");
		Matcher m = pat.matcher(oldLabel);
		while(m.find()){
			String s2replace = oldLabel.substring(m.start(), m.end());
			String replaceS = obdsql.getNode(s2replace).getLabel();
			if(replaceS == null)
				replaceS = s2replace.substring(s2replace.indexOf(":") + 1);
			label = label.replace(s2replace, replaceS);
		}
		label = label.replace("_", " ");
		return label;
	}
}
