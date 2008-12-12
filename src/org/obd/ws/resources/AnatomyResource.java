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
	private OBDQuery obdq;
	private Set<String> taxa;
	private Set<String> genes;
	private Set<String> genotypes;
	
	private int annotationCount;

	private final String IS_A_RELATION = "OBO_REL:is_a";
	private final String EXHIBITS_RELATION = "PHENOSCAPE:exhibits";
	private final String HAS_ALLELE_RELATION = "PHENOSCAPE:has_allele";

	public AnatomyResource(Context context, Request request, Response response) {
		super(context, request, response);

		this.obdsql = (Shard) this.getContext().getAttributes().get("shard");
		this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
		// this.getVariants().add(new Variant(MediaType.TEXT_HTML));
		this.termId = Reference.decode((String) (request.getAttributes()
				.get("termID")));
		obdq = new OBDQuery(obdsql);
		taxa = new HashSet<String>();
		genes = new HashSet<String>();
		genotypes = new HashSet<String>();
		annotationCount = 0;
		jObjs = new JSONObject();
		// System.out.println(termId);
	}

	public Representation getRepresentation(Variant variant) {

		Representation rep = null;

		try {
			if(!termId.startsWith("TAO:") && !termId.startsWith("ZFA:")){
				this.jObjs = null;
				getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST, "ERROR: The input parameter " +
						"is not a recognized anatomical entity");
				return null;
			}
			getAnatomyTermInfo(this.termId);
			if (this.jObjs == null) {
				getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND,
						"The search term was not found");
				return null;
			}
			getAnatomyTermSummary(this.termId);
			
			// final jsonObject will be assembled at this point
			JSONObject taxonAnnotations = new JSONObject();
			JSONObject genotypeAnnotations = new JSONObject();
			JSONObject geneAnnotations = new JSONObject();
			taxonAnnotations.put("annotation_count", this.annotationCount);
			taxonAnnotations.put("taxon_count", this.taxa.size());
			genotypeAnnotations.put("annotation_count", this.annotationCount);
			genotypeAnnotations.put("genotype_count", this.genotypes.size());
			geneAnnotations.put("annotation_count", this.annotationCount);
			geneAnnotations.put("gene_count", this.genes.size());
			this.jObjs.put("taxon_annotations", taxonAnnotations);
			this.jObjs.put("genotype_annotations", genotypeAnnotations);
			this.jObjs.put("gene_annotations", geneAnnotations);
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

	private void getAnatomyTermInfo(String termId2) throws IOException,
			SQLException, ClassNotFoundException, JSONException {
		if (obdsql.getNode(termId2) != null) { // we need a node with a name!
			String def = "";
			this.jObjs.put("id", termId2);
			this.jObjs.put("name", obdsql.getNode(termId2).getLabel());
			Collection<LiteralStatement> lstmts = obdsql
					.getLiteralStatementsByNode(termId2);
			for (LiteralStatement lstmt : lstmts) { // find the definition if it
													// exists
				if (lstmt.getRelationId().toLowerCase().contains("definition")) {
					def = lstmt.getTargetId();
				}
			}
			if (def.length() > 0)
				this.jObjs.put("definition", def);
		} else {
			this.jObjs = null;
		}

	}

	private void getAnatomyTermSummary(String termId) throws IOException,
			SQLException, ClassNotFoundException, JSONException, IllegalArgumentException {

		
		String relationId, nodeId, targetId;
		Set<Statement> stmts = obdq.genericTermSearch(termId);

		
		for (Statement stmt : stmts) {
			relationId = stmt.getRelationId();
			nodeId = stmt.getNodeId();
			targetId = stmt.getTargetId();
			if (!nodeId.equals(targetId)) {
				//System.out.println(stmt);
				++annotationCount;
				if (relationId.equals(EXHIBITS_RELATION)) {
					if (nodeId.contains("TTO:")) { // "Taxon exhibits Phenotype" statement
						taxa.add(stmt.getNodeId());
					} else if (nodeId.contains("GENO")) { // "Genotype exhibits Phenotype" statement
						genotypes.add(nodeId);
					//	System.out.println("Looking for gene for genotype: " + nodeId);
						getGeneForGenotype(nodeId);
					}
				} else if (relationId.equals(HAS_ALLELE_RELATION)) { // "Gene has allele Genotype" statement
					genes.add(nodeId);
				} else if (relationId.equals(IS_A_RELATION)) {
					if (stmt.getTargetId().equals(termId)) { // child node
				//		System.out.println("Looking for child node: " + nodeId);
						getAnatomyTermSummary(nodeId);
					}
				}
			}
		}
	}

	private void getGeneForGenotype(String genotypeId) {
		// TODO Auto-generated method stub
		Set<Statement> stmts = obdq.genericTermSearch(genotypeId);
		for(Statement stmt : stmts){
		//	System.out.println("Looking for gene in: " + stmt);
			if(stmt.getRelationId().equals(HAS_ALLELE_RELATION) &&
					stmt.getTargetId().equals(genotypeId)){
			//	System.out.println("Adding gene: " + stmt.getNodeId() + " for genotype: " + genotypeId);
				genes.add(stmt.getNodeId());
			}
		}
	}
}
