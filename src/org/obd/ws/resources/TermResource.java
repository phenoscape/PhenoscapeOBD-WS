package org.obd.ws.resources;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.nescent.informatics.OBDQuery;
import org.obd.model.LiteralStatement;
import org.obd.model.Node;
import org.obd.model.Statement;
import org.obd.query.Shard;
import org.obd.query.impl.OBDSQLShard;
import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Reference;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.Variant;

public class TermResource extends Resource {

	private final String termId;
	private JSONObject jObjs;
	private Shard obdsql;

	public TermResource(Context context, Request request, Response response,
			OBDSQLShard obdsql) {
		super(context, request, response);
		this.obdsql = obdsql;
		this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
		// this.getVariants().add(new Variant(MediaType.TEXT_HTML));
		this.termId = Reference.decode((String) (request.getAttributes()
				.get("termId")));

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
		String stringRep = "";

		try {
			this.jObjs = getTermInfo(this.termId);
			stringRep = this.renderJsonObjectAsString(this.jObjs, 0);
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
		rep = new JsonRepresentation(this.jObjs);
		System.out.println(stringRep);

		return rep;

	}

	private JSONObject getTermInfo(String termId) throws IOException,
			SQLException, ClassNotFoundException, JSONException {

		OBDQuery obdq = new OBDQuery(obdsql);

		Set<Statement> stmts = obdq.genericTermSearch(termId);
		JSONObject jsonObj = new JSONObject();

		Set<JSONObject> parents = new HashSet<JSONObject>();
		Set<JSONObject> children = new HashSet<JSONObject>();
		Set<JSONObject> otherRelations = new HashSet<JSONObject>();
		String def = "";
		Collection<LiteralStatement> lstmts = obdsql.getLiteralStatementsByNode(termId);
		for(LiteralStatement lstmt : lstmts){
			if(lstmt.getRelationId().toLowerCase().contains("definition")){
				def = lstmt.getTargetId();
			}
		}
		jsonObj.put("id", termId);
		jsonObj.put("name", obdsql.getNode(termId).getLabel());
		if(def.length() > 0)
			jsonObj.put("definition", def);
		for (Statement stmt : stmts) {
			String subj = stmt.getNodeId();
			String pred = stmt.getRelationId();
			String obj = stmt.getTargetId();
			Node objNode = obdsql.getNode(obj);
			if (pred != null && pred.length() > 0) {
				if (pred.contains("is_a") || pred.contains("part_of")) {
					if (subj.equals(termId)) {
						JSONObject parent = new JSONObject();
						parent.put("relation", pred);
						parent.put("id", obj);
						parent.put("name", obdsql.getNode(obj).getLabel());
						parents.add(parent);
					} else if (obj.equals(termId)) {
						JSONObject child = new JSONObject();
						child.put("relation", pred);
						child.put("id", subj);
						child.put("name", obdsql.getNode(subj).getLabel());
						children.add(child);
					}
				} else {
					JSONObject otherRelation = new JSONObject();
					otherRelation.put("relation", pred);
					otherRelation.put("id", obj);
					if (objNode != null) {
						otherRelation.put("name", obdsql.getNode(obj)
								.getLabel());
					} else {
						otherRelation.put("name", "unknown");
					}
					otherRelations.add(otherRelation);
				}
			}
		}
		jsonObj.put("parents", parents);
		jsonObj.put("children", children);
		jsonObj.put("otherRelations", otherRelations);

		return jsonObj;
	}

	private String renderJsonObjectAsString(JSONObject jo, int indentCt) throws JSONException {
		String output = "";
		String tabs = "";
		for(int ct = 0; ct < indentCt; ct++){
			tabs += "\t";
		}
		output += "{\n";
		
		String idPart, namePart, relationPart, defPart;
		if (jo.has("relation") && jo.get("relation") != null) {
			relationPart = "relation: " + (String) jo.get("relation") + "\n";
			output += tabs + relationPart;
		}
		if (jo.has("id") && jo.get("id") != null) {
			idPart = "id: " + (String) jo.get("id") + "\n";
			output += tabs + idPart;
		}
		if (jo.has("name") && jo.get("name") != null) {
			namePart = "name: " + (String) jo.get("name") + "\n";
			output += tabs + namePart;
		}
		if (jo.has("definition") && jo.get("definition") != null) {
			defPart = "definition: " + (String) jo.get("definition") + "\n";
			output += tabs + defPart;
		}

		if (jo.has("parents")) {
			JSONArray parents = (JSONArray) jo.get("parents");
			if (parents != null) {
				output += "parents: \n";
				for(int i = 0; i < parents.length(); i++){
					JSONObject parent = parents.getJSONObject(i);
					output += renderJsonObjectAsString(parent, indentCt);
				}
			}
		}
		if (jo.has("children")) {
			JSONArray children = (JSONArray) jo.get("children");
			if (children != null) {
				output += "children: \n";
				for(int j = 0; j < children.length(); j++){
					JSONObject child = children.getJSONObject(j);
					output += renderJsonObjectAsString(child, indentCt);
				}
			}
		}
		if (jo.has("otherRelations")) {
			JSONArray others = (JSONArray) jo.get("otherRelations");
			if (others != null) {
				output += "links: \n";
				for(int k = 0; k < others.length(); k++){
					JSONObject other = others.getJSONObject(k);
					output += renderJsonObjectAsString(other, indentCt);
				}
			}
		}
		output += tabs + "}\n";
		return output;
	}
}
