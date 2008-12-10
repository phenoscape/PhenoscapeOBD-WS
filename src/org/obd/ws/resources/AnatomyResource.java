package org.obd.ws.resources;

import java.io.IOException;
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

public class AnatomyResource extends Resource {

	private final String termId;
	private JSONObject jObjs;
	private Shard obdsql;

	public AnatomyResource(Context context, Request request, Response response) {
		super(context, request, response);
		this.obdsql = (Shard) this.getContext().getAttributes().get("shard");
		this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
		// this.getVariants().add(new Variant(MediaType.TEXT_HTML));
		this.termId = Reference.decode((String) (request.getAttributes()
				.get("termID")));
		// System.out.println(termId);
	}

	public Representation getRepresentation(Variant variant) {

		Representation rep = null;

		try {
			this.jObjs = getAnatomyTermInfo(this.termId);
			if (this.jObjs.get("name").toString().equals("not found")) {
				getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND,
						"The search term was not found");
			}
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
		return rep;

	}

	private JSONObject getAnatomyTermInfo(String termId) throws IOException,
			SQLException, ClassNotFoundException, JSONException {

		OBDQuery obdq = new OBDQuery(obdsql);
		JSONObject jsonObj = new JSONObject();
		
		Set<Statement> stmts = obdq.genericSearch(null, termId, null);	
		String def = "";
		Collection<LiteralStatement> lstmts = obdsql.getLiteralStatementsByNode(termId);
		for (LiteralStatement lstmt : lstmts) {
			if (lstmt.getRelationId().toLowerCase().contains("definition")) {
				def = lstmt.getTargetId();
			}
		}

		if (obdsql.getNode(termId) != null) {
			jsonObj.put("id", termId);
			jsonObj.put("name", obdsql.getNode(termId).getLabel());
			if (def.length() > 0)
				jsonObj.put("definition", def);

			for(Statement stmt : stmts){
				System.out.println(stmt);
			}
		}
		else{
			jsonObj.put("name", "not found");
		}

		return jsonObj;
	}
}
