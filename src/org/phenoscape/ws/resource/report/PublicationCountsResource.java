package org.phenoscape.ws.resource.report;

import java.sql.SQLException;

import org.phenoscape.obd.model.Term;
import org.phenoscape.obd.model.Vocab.TTO;
import org.phenoscape.ws.resource.AbstractPhenoscapeResource;
import org.restlet.data.CharacterSet;
import org.restlet.data.Language;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;

public class PublicationCountsResource extends AbstractPhenoscapeResource {

    @Get("tsv")
    public Representation getTable() {
        try {
            final StringBuffer result = new StringBuffer();
            result.append("All");
            for (String taxonID : TTO.HIGHER_LEVEL_TAXA) {
                result.append("\t");
                final Term taxon = this.getDataStore().getTerm(taxonID);
                result.append(taxon.getLabel());
            }
            result.append(System.getProperty("line.separator"));
            result.append(this.getDataStore().getCountOfAnnotatedPublications(null));
            for (String taxonID : TTO.HIGHER_LEVEL_TAXA) {
                result.append("\t");
                final int count = this.getDataStore().getCountOfAnnotatedPublications(taxonID);
                result.append(count);
            }
            result.append(System.getProperty("line.separator"));
            return new StringRepresentation(result.toString(), MediaType.TEXT_TSV, Language.DEFAULT, CharacterSet.UTF_8);
        } catch (SQLException e) {
            log().error("Error querying publication counts", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        }
    }

}
