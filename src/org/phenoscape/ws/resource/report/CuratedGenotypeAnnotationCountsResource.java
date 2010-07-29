package org.phenoscape.ws.resource.report;

import java.sql.SQLException;

import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.query.AnnotationsQueryConfig;
import org.phenoscape.obd.query.GeneAnnotationsQueryConfig;
import org.phenoscape.ws.resource.AbstractPhenoscapeResource;
import org.restlet.data.CharacterSet;
import org.restlet.data.Language;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public class CuratedGenotypeAnnotationCountsResource extends AbstractPhenoscapeResource {

    private AnnotationsQueryConfig config = new GeneAnnotationsQueryConfig();
       
       @Override
       protected void doInit() throws ResourceException {
           super.doInit();
           try {
               this.config = this.initializeQueryConfig(this.getJSONQueryValue("query", new JSONObject()));
           } catch (JSONException e) {
               log().error("Bad JSON format", e);
               throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, e);
           }
       }
       
       @Get("tsv")
       public Representation getTable() {
           try {
               final StringBuffer result = new StringBuffer();
               result.append("Genotype Annotations");
               result.append(System.getProperty("line.separator"));
               result.append(this.getDataStore().getCountOfGenotypeAnnotations(this.config));
               result.append(System.getProperty("line.separator"));
               return new StringRepresentation(result.toString(), MediaType.TEXT_TSV, Language.DEFAULT, CharacterSet.UTF_8);
           } catch (SQLException e) {
               log().error("Error querying genotype annotation counts", e);
               this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
               return null;
           }
       }
       
}
