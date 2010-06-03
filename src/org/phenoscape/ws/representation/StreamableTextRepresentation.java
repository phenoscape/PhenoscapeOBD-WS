package org.phenoscape.ws.representation;

import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;

import org.restlet.data.MediaType;
import org.restlet.representation.WriterRepresentation;

public class StreamableTextRepresentation extends WriterRepresentation {

    private final Iterator<? extends Object> items;
    private static final String SEPARATOR = System.getProperty("line.separator"); 

    public StreamableTextRepresentation(Iterator<?> items) {
        this(items, MediaType.TEXT_PLAIN);
    }

    public StreamableTextRepresentation(Iterator<?> items, MediaType mediaType) {
        super(mediaType);
        this.items = items;
    }

    @Override
    public void write(Writer writer) throws IOException {
        while (this.items.hasNext()) {
            writer.write(this.items.next().toString());
            if (this.items.hasNext()) {
                writer.write(this.getItemSeparator());   
            }
        }
    }

    protected String getItemSeparator() {
        return SEPARATOR;
    }

}
