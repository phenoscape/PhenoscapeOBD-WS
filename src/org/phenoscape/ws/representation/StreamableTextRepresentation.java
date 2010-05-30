package org.phenoscape.ws.representation;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.Iterator;

import org.restlet.data.MediaType;
import org.restlet.representation.OutputRepresentation;

public class StreamableTextRepresentation extends OutputRepresentation {
    
    private final Iterator<? extends Object> items;
    private String characterEncoding = "UTF-8";
    private static final String SEPARATOR = System.getProperty("line.separator"); 
    
    public StreamableTextRepresentation(Iterator<? extends Object> items) {
        this(items, MediaType.TEXT_PLAIN);
    }

    public StreamableTextRepresentation(Iterator<? extends Object> items, MediaType mediaType) {
        super(mediaType);
        this.items = items;
    }

    @Override
    public void write(OutputStream stream) throws IOException {
        final Writer writer = new BufferedWriter(new OutputStreamWriter(stream, Charset.forName("UTF-8")));
        while (this.items.hasNext()) {
            writer.write(this.items.next().toString());
            if (this.items.hasNext()) {
                writer.write(this.getItemSeparator());   
            }
        }
        writer.close();
    }
    
    public String getCharacterEncoding() {
        return this.characterEncoding;
    }
    
    public void setCharacterEncoding(String encoding) {
        this.characterEncoding = encoding;
    }
    
    protected String getItemSeparator() {
        return SEPARATOR;
    }

}
