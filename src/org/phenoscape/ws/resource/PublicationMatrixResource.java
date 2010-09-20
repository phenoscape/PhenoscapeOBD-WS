package org.phenoscape.ws.resource;

import java.io.IOException;
import java.io.Writer;
import java.sql.SQLException;

import javax.xml.parsers.ParserConfigurationException;

import org.biojava.bio.seq.io.ParseException;
import org.biojavax.bio.phylo.io.nexus.NexusFile;
import org.biojavax.bio.phylo.io.nexus.NexusFileFormat;
import org.nexml.model.Document;
import org.restlet.data.MediaType;
import org.restlet.data.Reference;
import org.restlet.data.Status;
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
    
    @Get("txt")
    public Representation getNEXUSRepresentation() {
        try {
            final NexusFile nexusFile = this.getDataStore().getNexusFileForPublication(this.publicationID);
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
            final Document nexmlDoc = this.getDataStore().getNexmlDocumentForPublication(this.publicationID);
            return new StringRepresentation(nexmlDoc.getXmlString(), MediaType.APPLICATION_XML);
        } catch (ParserConfigurationException e) {
            log().error("Error creating NeXML document", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        }
    }
    
}
