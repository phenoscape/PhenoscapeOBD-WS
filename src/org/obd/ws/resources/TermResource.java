package org.obd.ws.resources;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;
import org.nescent.informatics.OBDQuery;
import org.obd.model.LiteralStatement;
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

public class TermResource extends Resource {

	private final String termId;
	private JSONObject jObjs;
	private Shard obdsql;
	private Connection conn;
	
	public TermResource(Context context, Request request, Response response) {
		super(context, request, response);
		this.obdsql = (Shard) this.getContext().getAttributes().get("shard");
		this.conn = (Connection)getContext().getAttributes().get("conn");
		this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
		// this.getVariants().add(new Variant(MediaType.TEXT_HTML));
		this.termId = Reference.decode((String) (request.getAttributes()
				.get("termID")));
		// System.out.println(termId);
	}

	// this constructor is to be used only for testing purposes
	@Deprecated
	public TermResource(Shard obdsql, String termId) {
		this.termId = termId;
		this.obdsql = obdsql;
		this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
	}

	public Representation getRepresentation(Variant variant) {

		Representation rep = null;
		// String stringRep = "";

		try {
			this.jObjs = getTermInfo(this.termId);
			if (this.jObjs == null) {
				getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND,
						"The search term was not found");
				return null;
			}
			// stringRep = this.renderJsonObjectAsString(this.jObjs, 0);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// System.out.println(stringRep);
		rep = new JsonRepresentation(this.jObjs);
		return rep;

	}

	private JSONObject getTermInfo(String termId) throws IOException,
			SQLException, ClassNotFoundException, JSONException {

		OBDQuery obdq = new OBDQuery(obdsql, conn);

		Set<Statement> stmts = obdq.genericTermSearch(termId);
		JSONObject jsonObj = new JSONObject();
		if(termId.indexOf(":") < 0){
			return null;
		}
		if (obdsql.getNode(termId) != null) {
			
			String prefix = termId.substring(0, termId.indexOf(":"));
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
			String name = obdsql.getNode(termId).getLabel();
																		
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
			for (Statement stmt : stmts) {
				String subj = stmt.getNodeId();
				String pred = stmt.getRelationId();
				String obj = stmt.getTargetId();
				if (pred != null && pred.length() > 0) {
					if (subj.equals(termId) &&
							obj.substring(0, obj.indexOf(":")).equals(prefix)) {
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
					} else if (obj.equals(termId) &&
							subj.substring(0, obj.indexOf(":")).equals(prefix)) {
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
			}

			jsonObj.put("parents", parents);
			jsonObj.put("children", children);
			// jsonObj.put("otherRelations", otherRelations);
		} else {
			jsonObj = null;
		}
		return jsonObj;
	}
}
