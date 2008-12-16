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
import org.nescent.informatics.OBDQuery;
import org.obd.model.CompositionalDescription;
import org.obd.model.LinkStatement;
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
	private OBDQuery obdq;
	private Set<String> taxa;
	private Set<String> genes;
	private Set<String> genotypes;
	private Set<String> qualities;

	private Map<String, Set<String>> qualityToTaxonMap;
	private Map<String, Set<String>> qualityToGenotypeMap;
	private Map<String, Set<String>> qualityToGeneMap;

	private String eqCombo;

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
		qualities = new HashSet<String>();
		qualityToTaxonMap = new HashMap<String, Set<String>>();
		qualityToGenotypeMap = new HashMap<String, Set<String>>();
		qualityToGeneMap = new HashMap<String, Set<String>>();
		annotationCount = 0;
		jObjs = new JSONObject();
		// System.out.println(termId);
	}

	public Representation getRepresentation(Variant variant) {

		Representation rep = null;

		try {
			if (!termId.startsWith("TAO:") && !termId.startsWith("ZFA:")) {
				this.jObjs = null;
				getResponse().setStatus(
						Status.CLIENT_ERROR_BAD_REQUEST,
						"ERROR: The input parameter "
								+ "is not a recognized anatomical entity");
				return null;
			}
			if (obdsql.getNode(this.termId) != null) {
				getAnatomyTermSummary(this.termId);

				if (qualityToTaxonMap != null) {
					for (String quality : qualityToTaxonMap.keySet()) {
						for (String taxon : qualityToTaxonMap.get(quality)) {
							System.out.println(taxon + " exhibits " + quality);
						}
					}
				}

				if (qualityToGenotypeMap != null) {
					for (String quality : qualityToGenotypeMap.keySet()) {
						for (String genotype : qualityToGenotypeMap
								.get(quality)) {
							System.out.println(genotype + " exhibits "
									+ quality);
						}
					}
				}

				if (qualityToGeneMap != null) {
					for (String quality : qualityToGeneMap.keySet()) {
						for (String gene : qualityToGeneMap.get(quality)) {
							System.out.println(gene + " encodes " + quality);
						}
					}
				}
				// final jsonObject will be assembled at this point
				JSONObject taxonAnnotations = new JSONObject();
				JSONObject genotypeAnnotations = new JSONObject();
				JSONObject geneAnnotations = new JSONObject();
				taxonAnnotations.put("annotation_count", this.annotationCount);
				taxonAnnotations.put("taxon_count", this.taxa.size());
				genotypeAnnotations.put("annotation_count",
						this.annotationCount);
				genotypeAnnotations
						.put("genotype_count", this.genotypes.size());
				geneAnnotations.put("annotation_count", this.annotationCount);
				geneAnnotations.put("gene_count", this.genes.size());
				this.jObjs.put("taxon_annotations", taxonAnnotations);
				this.jObjs.put("genotype_annotations", genotypeAnnotations);
				this.jObjs.put("gene_annotations", geneAnnotations);

			} else {
				getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND,
						"The search term was not found");
				return null;
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

	private void getAnatomyTermSummary(String termId) throws IOException,
			SQLException, ClassNotFoundException, JSONException,
			IllegalArgumentException {

		// start working with the given anatomical feature
		String nodeId, targetId;

		Collection<Statement> stmts = obdq.getStatementsWithTargetOfRelation(
				termId, EXHIBITS_RELATION);

		for (Statement stmt : stmts) {
			nodeId = stmt.getNodeId();
			targetId = stmt.getTargetId();
			++annotationCount;

			if (nodeId.contains("TTO:")) { // "Taxon exhibits Phenotype"
											// statement
				taxa.add(stmt.getNodeId());
				qualityToTaxonMap.put(targetId, taxa);

			} else if (nodeId.contains("GENO")) { // "Genotype exhibits Phenotype"
													// statement
				genotypes.add(nodeId);
				qualityToGenotypeMap.put(targetId, genotypes);
				getGeneForGenotype(targetId, nodeId);
			}
		}

		// look for subclasses of the input anatomical feature and find "their"
		// qualities and genes
		if (obdq.getStatementsWithTargetOfRelation(termId, IS_A_RELATION)
				.size() > 0) {
			for (Statement scStmt : obdq.getStatementsWithTargetOfRelation(
					termId, IS_A_RELATION)) {
				getAnatomyTermSummary(scStmt.getNodeId());
			}
		}
	}


	/**
	 * A method to find the Gene a Genotype is an allele of
	 * 
	 * @param genotypeId
	 */
	private void getGeneForGenotype(String quality, String genotypeId) {
		Collection<Statement> stmts = obdq.genericTermSearch(genotypeId);
		for (Statement stmt : stmts) {
			if (stmt.getRelationId().equals(HAS_ALLELE_RELATION)) {
				genes.add(stmt.getNodeId());
			}
		}
		qualityToGeneMap.put(quality, genes);
	}
}
