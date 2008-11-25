package org.obd.ws.resources;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;
import org.nescent.informatics.OBDQuery;
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

public class AutoCompleteResource extends Resource {

	private final String text;
	private Map<String, String> options;
	private JSONObject jObjs;
	private Shard obdsql;

	public AutoCompleteResource(Context context, Request request,
			Response response, OBDSQLShard obdsql) {
		super(context, request, response);
		this.obdsql = obdsql;
		this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
		// this.getVariants().add(new Variant(MediaType.TEXT_HTML));
		this.text = Reference.decode((String) (request.getAttributes()
				.get("text")));

	}

	// this constructor is to be used only for testing purposes
	@Deprecated
	public AutoCompleteResource(Shard obdsql, String text,
			Map<String, String> options) {
		this.text = text;
		this.obdsql = obdsql;
		this.options = options;
		this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
	}

	public Representation getRepresentation(Variant variant) {

		Representation rep = null;
		String stringRep = "";

		try {
			this.jObjs = getTextMatches(this.text, this.options);
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
		rep = new JsonRepresentation(this.jObjs);
		System.out.println(stringRep);

		return rep;

	}

	private JSONObject getTextMatches(String text, Map<String, String> options)
			throws IOException, SQLException, ClassNotFoundException,
			JSONException {

		JSONObject jObj = new JSONObject();
		OBDQuery obdq = new OBDQuery(obdsql);
		String byNameOption = options.get("byName");
		String bySynonymOption = ((options.get("bySynonym") == null || options
				.get("bySynonym").length() == 0) ? "false" : options
				.get("bySynonym"));
		String byDefinitionOption = ((options.get("byDefinition") == null || options
				.get("byDefinition").length() == 0) ? "false" : options
				.get("byDefinition"));
		String byOntologyOption = (options.get("byOntology") == null || options
				.get("byOntology").length() == 0) ? "all" : options
				.get("byOntology");

		Set<Statement> stmts = obdq.getCompletionsForSearchTerm(text, byNameOption, bySynonymOption, byDefinitionOption, byOntologyOption);
		//TODO
		return jObj;
	}
}
