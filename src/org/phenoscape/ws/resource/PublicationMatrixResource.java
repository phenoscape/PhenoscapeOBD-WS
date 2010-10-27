package org.phenoscape.ws.resource;

import java.io.IOException;
import java.io.Writer;
import java.sql.SQLException;

import javax.xml.parsers.ParserConfigurationException;

import org.biojava.bio.seq.io.ParseException;
import org.biojavax.bio.phylo.io.nexus.NexusFile;
import org.biojavax.bio.phylo.io.nexus.NexusFileFormat;
import org.json.JSONException;
import org.json.JSONObject;
import org.nexml.model.Document;
import org.phenoscape.obd.model.Matrix;
import org.phenoscape.obd.model.MatrixUtil;
import org.restlet.data.MediaType;
import org.restlet.data.Reference;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.WriterRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public class PublicationMatrixResource extends AbstractPhenoscapeResource {

    private String publicationID = null;

    @Override
    protected void doInit() throws ResourceException {
        super.doInit();
        this.publicationID = Reference.decode((String)(this.getRequestAttributes().get("publicationID")));
    }

    @Get("json")
    public Representation getJSONRepresentation() {
        try {
            final Matrix matrix = this.getDataStore().getMatrixForPublication(this.publicationID);
            final JSONObject json = MatrixUtil.translateToJSON(matrix);
            return new JsonRepresentation(json);
        } catch (SQLException e) {
            log().error("Error querying database", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        } catch (JSONException e) {
            log().error("Error creating JSON document", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        }
    }

    @Get("txt")
    public Representation getNEXUSRepresentation() {
        try {
            final Matrix matrix = this.getDataStore().getMatrixForPublication(this.publicationID);
            final NexusFile nexusFile = MatrixUtil.translateToNEXUS(matrix);
            log().debug("Got NEXUS file: " + nexusFile);
            return new WriterRepresentation(MediaType.TEXT_PLAIN) {
                @Override
                public void write(Writer writer) throws IOException {
                    NexusFileFormat.writeWriter(writer, nexusFile);
                }
            };
        } catch (SQLException e) {
            log().error("Error querying database", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        } catch (ParseException e) {
            log().error("Error creating NEXUS document", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        }
    }

    @Get("xml")
    public Representation getNeXMLRepresentation() {
        try {
            final Matrix matrix = this.getDataStore().getMatrixForPublication(this.publicationID);
            final Document nexmlDoc = MatrixUtil.translateToNeXML(matrix);
            return new StringRepresentation(nexmlDoc.getXmlString(), MediaType.APPLICATION_XML);
        } catch (ParserConfigurationException e) {
            log().error("Error creating NeXML document", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        } catch (SQLException e) {
            log().error("Error querying database", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        }
    }

}
