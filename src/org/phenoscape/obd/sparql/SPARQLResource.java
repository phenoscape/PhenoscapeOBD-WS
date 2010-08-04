package org.phenoscape.obd.sparql;

import java.io.StringReader;

import name.levering.ryan.sparql.common.RdfBindingRow;
import name.levering.ryan.sparql.common.RdfBindingSet;
import name.levering.ryan.sparql.model.Query;
import name.levering.ryan.sparql.model.SelectQuery;
import name.levering.ryan.sparql.parser.ParseException;
import name.levering.ryan.sparql.parser.SPARQLParser;

import org.apache.commons.lang.ObjectUtils;
import org.phenoscape.ws.resource.AbstractPhenoscapeResource;
import org.restlet.data.CharacterSet;
import org.restlet.data.Language;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public class SPARQLResource extends AbstractPhenoscapeResource {

    private Query query;

    @Override
    protected void doInit() throws ResourceException {
        super.doInit();
        final String sparql = this.getFirstQueryValue("query");
        if (sparql == null) {
            log().debug("No query");
            throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST);
        } else {
            try {
                this.query = SPARQLParser.parse(new StringReader(sparql));
            } catch (ParseException e) {
                log().debug("Error parsing sparql", e);
                throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, e);
            }
        }
    }

    @Get("tsv")
    public Representation getTabDelimitedRepresentation() {
        final OBDRDFSource sparqler = new OBDRDFSource(this.getDataSource());
        if (this.query instanceof SelectQuery) {
            SelectQuery select = (SelectQuery)this.query;
            RdfBindingSet results = select.execute(sparqler);
            final StringBuffer result = new StringBuffer();
            for (Object header : results.getVariables()) {
                result.append(header.toString());
                result.append("\t");
            }
            result.append(System.getProperty("line.separator"));
            for (Object item : results) {
                if (item != null) {
                    final RdfBindingRow row = (RdfBindingRow)item;
                    for (Object value : row.getValues()) {
                        result.append(ObjectUtils.toString(value));
                        result.append("\t");
                    }
                }
                result.append(System.getProperty("line.separator"));
            }
            return new StringRepresentation(result.toString(), MediaType.TEXT_TSV, Language.DEFAULT, CharacterSet.UTF_8);
        }
        return null;
    }

}
