package org.obd.ws.resources;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;
import org.nescent.informatics.OBDQuery;
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

public class AnnotationSummaryResource extends Resource {
	
	private String subject_id;
	private String entity_id;
	private String quality_id; 
	private String publication_id;
	private int examples_count = 2;
	
	private Map<String, String> parameters;
	
	private JSONObject jObjs;
	private Shard obdsql;
	private Connection conn;
	private OBDQuery obdq;
	
	private Set<String> geneSet, taxonSet;
	
	public AnnotationSummaryResource(Context context, Request request, Response response) {
		super(context, request, response);

		this.obdsql = (Shard) this.getContext().getAttributes().get("shard");
		this.conn = (Connection)this.getContext().getAttributes().get("conn");
		this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
		// this.getVariants().add(new Variant(MediaType.TEXT_HTML));
		if(request.getResourceRef().getQueryAsForm().getFirstValue("subject_id") != null){
			this.subject_id = Reference.decode((String) (request.getAttributes().get("subject_id")));
		}
		if(request.getResourceRef().getQueryAsForm().getFirstValue("entity_id") != null){
			this.entity_id = Reference.decode((String)(request.getAttributes().get("entity_id")));
		}
		if(request.getResourceRef().getQueryAsForm().getFirstValue("quality_id") != null){
			this.quality_id = Reference.decode((String)(request.getAttributes().get("quality_id")));
		}
		if(request.getResourceRef().getQueryAsForm().getFirstValue("publication_id") != null){
			this.publication_id = Reference.decode((String)(request.getAttributes().get("publication_id")));
		}
		if(request.getResourceRef().getQueryAsForm().getFirstValue("examples_count") != null){
			this.examples_count = Integer.parseInt(Reference.decode((String)(request.getAttributes().get("examples_count"))));
		}
		String aq = (String)this.getContext().getAttributes().get("anatomyQuery");
		String tq = (String)this.getContext().getAttributes().get("taxonQuery");
		String gq = (String)this.getContext().getAttributes().get("geneQuery");
		
		obdq = new OBDQuery(obdsql, conn, new String[]{aq, tq, gq});
		jObjs = new JSONObject();
		parameters = new HashMap<String, String>();
		// System.out.println(termId);
	}

	/**
	 * This is an updated method for the new code that executes faster queries DT:03-16-09
	 */
	public Representation getRepresentation(Variant variant) {

		Representation rep;

		if (subject_id != null && !subject_id.startsWith("TTO:") && !subject_id.contains("GENE:")) {
			this.jObjs = null;
			getResponse().setStatus(
					Status.CLIENT_ERROR_BAD_REQUEST,
					"ERROR: The input parameter for subject "
							+ "is not a recognized taxon or gene");
			return null;
		}
		if(entity_id != null && !entity_id.startsWith("TAO:") && !entity_id.startsWith("ZFA:")){
			this.jObjs = null;
			getResponse().setStatus(
					Status.CLIENT_ERROR_BAD_REQUEST,
					"ERROR: The input parameter for entity "
							+ "is not a recognized anatomical entity");
			return null;
		}
		if(quality_id != null && !quality_id.startsWith("PATO:")){
			this.jObjs = null;
			getResponse().setStatus(
					Status.CLIENT_ERROR_BAD_REQUEST,
					"ERROR: The input parameter for quality "
							+ "is not a recognized PATO quality");
			return null;
		}
			
		//TODO Publication ID check
		
		parameters.put("entity_id", entity_id);
		parameters.put("quality_id", quality_id);
		parameters.put("subject_id", subject_id);
		//parameters.put("publication_id", publication_id);
		
		for(String key : parameters.keySet()){
			if(parameters.get(key) != null){
				if(obdsql.getNode(parameters.get(key)) == null){
					getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND,
					"No annotations were found with the specified search parameters");
					return null;
				}
			}
		}
		rep = new JsonRepresentation(this.jObjs);
		return rep;
	}

	private Map<String, Map<String, List<List<Map<String, String>>>>> getAnnotationSummary(String subject_id, String entity_id, String quality_id) 
				throws IOException, SQLException, ClassNotFoundException, JSONException, IllegalArgumentException{
		
		Map<String, String> nodeProps;
		String relId, target, character = null, taxon = null, entity = null, quality = null;
		String query, searchTerm;
		if(subject_id != null){
			searchTerm = subject_id;
			if(subject_id.contains("GENE"))
				query = obdq.getGeneQuery();
			else
				query = obdq.getTaxonQuery();
		}
		else{
			if(entity_id == null)
				searchTerm = "TAO:0100000";
			else 
				searchTerm = entity_id;
			query = obdq.getAnatomyQuery();
		}
		
		for(Node node : obdq.executeQuery(query, searchTerm)){
			nodeProps = new HashMap<String, String>();
			for(Statement stmt : node.getStatements()){
				relId = stmt.getRelationId();
				target = stmt.getTargetId();
				nodeProps.put(relId, target);
			} 
			nodeProps.put("id", node.getId());
			character = nodeProps.get("hasCharacter");
			taxon = nodeProps.get("exhibitedBy");
			entity = nodeProps.get("inheresIn");
			quality = nodeProps.get("hasState");
			
			
			
		}
		return null;
	}
}
