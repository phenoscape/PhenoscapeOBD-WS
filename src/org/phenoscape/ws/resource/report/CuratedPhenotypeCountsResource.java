package org.phenoscape.ws.resource.report;

import java.sql.SQLException;

import org.phenoscape.ws.resource.AbstractPhenoscapeResource;
import org.restlet.data.CharacterSet;
import org.restlet.data.Language;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;

public class CuratedPhenotypeCountsResource extends AbstractPhenoscapeResource {

    @Get("tsv")
    public Representation getTable() {
        try {
            final StringBuffer result = new StringBuffer();
            result.append("Count of curated phenotypes");
            result.append(System.getProperty("line.separator"));
            result.append(this.getDataStore().getCountOfAllCuratedPhenotypes());
            result.append(System.getProperty("line.separator"));
            return new StringRepresentation(result.toString(), MediaType.TEXT_TSV, Language.DEFAULT, CharacterSet.UTF_8);
        } catch (SQLException e) {
            log().error("Error querying phenotype counts", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        }
    }
    
}
