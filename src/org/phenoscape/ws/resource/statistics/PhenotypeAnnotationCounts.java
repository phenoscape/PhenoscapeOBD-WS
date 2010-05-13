package org.phenoscape.ws.resource.statistics;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public class PhenotypeAnnotationCounts extends AbstractStatisticsGenerator{

    private Connection conn;	
    private JSONObject jObj;

    private final String queryForCountOfEvolutionaryAnnotations = 
        "SELECT COUNT(*) AS COUNT FROM dw_taxon_phenotype_table";

    private final String queryForCountOfDistinctEvolutionaryPhenotypes = 
        "SELECT COUNT(DISTINCT phenotype_nid) AS COUNT FROM dw_taxon_phenotype_table";

    private final String queryForCountOfDistinctTaxaWithPhenotypes = 
        "SELECT COUNT(DISTINCT taxon_nid) AS COUNT FROM dw_taxon_phenotype_table";

    private final String queryForCountOfGenePhenotypeAnnotations = 
        "SELECT DISTINCT gene_nid, phenotype_nid FROM dw_gene_genotype_phenotype_table";

    private final String queryForCountOfGenes = 
        "SELECT COUNT(DISTINCT gene_nid) AS COUNT FROM dw_gene_genotype_phenotype_table";

    private final String queryForCountOfDevelopmentalPhenotypes = 
        "SELECT COUNT(DISTINCT phenotype_nid) AS COUNT FROM dw_gene_genotype_phenotype_table";

    @Get("json")
    public Representation getJSONRepresentation() throws ResourceException {
        try {
            connectShardToDatabase();
            conn = this.getShard().getConnection();
            jObj = new JSONObject();

            getCountOfEvolutionaryAnnotations();
            getCountOfEvolutionaryPhenotypes();
            getCountOfEvolutionaryTaxa();

            getCountOfGeneToPhenotypeAnnotations();
            getCountOfDevelopmentalPhenotypes();
            getCountOfGenes();

            return new JsonRepresentation(jObj);
        } catch (SQLException sqle) {
            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, "[SQL EXCEPTION] Something broke server side. Consult server logs");
        } catch (ClassNotFoundException e) {
            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, "[CLASS NOT FOUND EXCEPTION] Something broke server side. Consult server logs");
        } catch (JSONException jsone) {
            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, "[JSON EXCEPTION] Something broke while assembling the JSON");
        } finally {
            disconnectShardFromDatabase();
        }
        return null;
    }

    private void getCountOfEvolutionaryAnnotations() throws JSONException {
        int countOfEvolutionaryAnnotations = 0;
        try {
            PreparedStatement ps = conn.prepareStatement(queryForCountOfEvolutionaryAnnotations);
            ResultSet rs = ps.executeQuery();
            while (rs.next())
                countOfEvolutionaryAnnotations = rs.getInt(1);
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        jObj.put("countOfEvolutionaryAnnotations", countOfEvolutionaryAnnotations);
    }

    private void getCountOfEvolutionaryTaxa() throws JSONException{
        int countOfEvolutionaryTaxa = 0;
        try {
            PreparedStatement ps = conn.prepareStatement(queryForCountOfDistinctTaxaWithPhenotypes);
            ResultSet rs = ps.executeQuery();
            while (rs.next())
                countOfEvolutionaryTaxa = rs.getInt(1);
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        jObj.put("countOfEvolutionaryTaxa", countOfEvolutionaryTaxa);
    }

    private void getCountOfEvolutionaryPhenotypes() throws JSONException{
        int countOfEvolutionaryPhenotypes = 0;
        try {
            PreparedStatement ps = conn.prepareStatement(queryForCountOfDistinctEvolutionaryPhenotypes);
            ResultSet rs = ps.executeQuery();
            while (rs.next())
                countOfEvolutionaryPhenotypes = rs.getInt(1);
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        jObj.put("countOfEvolutionaryPhenotypes", countOfEvolutionaryPhenotypes);
    }

    private void getCountOfGeneToPhenotypeAnnotations() throws JSONException {
        int countOfGeneToPhenotypeAnnotations = 0;
        try {
            PreparedStatement ps = conn.prepareStatement(queryForCountOfGenePhenotypeAnnotations);
            ResultSet rs = ps.executeQuery();
            while (rs.next())
                ++countOfGeneToPhenotypeAnnotations;
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        jObj.put("countOfGenePhenotypeAnnotations", countOfGeneToPhenotypeAnnotations);
    }

    private void getCountOfGenes() throws JSONException{
        int countOfGenes = 0;
        try {
            PreparedStatement ps = conn.prepareStatement(queryForCountOfGenes);
            ResultSet rs = ps.executeQuery();
            while (rs.next())
                countOfGenes = rs.getInt(1);
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        jObj.put("countOfGenes", countOfGenes);
    }

    private void getCountOfDevelopmentalPhenotypes() throws JSONException{
        int countOfDevelopmentalPhenotypes = 0;
        try {
            PreparedStatement ps = conn.prepareStatement(queryForCountOfDevelopmentalPhenotypes);
            ResultSet rs = ps.executeQuery();
            while (rs.next())
                countOfDevelopmentalPhenotypes = rs.getInt(1);
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        jObj.put("countOfDevelopmentalPhenotypes", countOfDevelopmentalPhenotypes);
    }
}
