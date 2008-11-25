package org.obd.ws.resources;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;

import org.json.JSONException;
import org.json.JSONObject;
import org.nescent.informatics.OBDQuery;
import org.obd.model.Node;
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
	private String[] options;
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
									String... options) {
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

	private JSONObject getTextMatches(String text, String... options)
			throws IOException, SQLException, ClassNotFoundException,
			JSONException {
		
		JSONObject jObj = new JSONObject();
		OBDQuery obdq = new OBDQuery(obdsql);
		String byNameOption = options[0];
		String bySynonymOption = ((options[1] == null || options[0].length() == 0) ? 
									"false" : options[1]);
		String byDefinitionOption = ((options[2] == null || options[2].length() == 0) ? 
									"false" : options[2]);
		String byOntologyOption = (options[3] == null || options[3].length() == 0) ? 
									"all" : options[3];

		if(!Boolean.parseBoolean(byNameOption)){
			throw new IllegalArgumentException();
		}
		int i = 0;
		Collection<Node> nodes = obdq.getCompletionsForSearchTerm(text, new String[]{byNameOption, bySynonymOption, byDefinitionOption, byOntologyOption});
		for(Node node : nodes){
			if(node.getId().contains(":"))
				System.out.println(++i + ". Search Term:" + text + "\t" + node.getId() + "\t" + node.getLabel());
		}
	//	if(Boolean.parseBoolean(byDefinitionOption)){
	//		Collection<LiteralStatement> lss = obdsql.getLiteralStatementsByNode(text);
	//		if(lss.size() > 0){
	//			LiteralStatement ls = lss.iterator().next();
	//			System.out.println(ls);
	//		}
	//	}
		//TODO UPDATE THIS TOMORROW - START HERE WED NOV 26, 2008
		return jObj;
	}
}
