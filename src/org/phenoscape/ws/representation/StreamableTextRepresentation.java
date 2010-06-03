package org.phenoscape.ws.representation;

import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;

import org.restlet.data.MediaType;
import org.restlet.representation.WriterRepresentation;

/**
 * A Representation which can be used for very large datasets without requiring that the full dataset is 
 * in memory as a String all at once.  The provided Iterator can be implemented to retrieve data items on demand.  
 * @author Jim Balhoff
 */
public class StreamableTextRepresentation extends WriterRepresentation {

    private final Iterator<? extends Object> items;
    private static final String SEPARATOR = System.getProperty("line.separator"); 

    /**
     * Create a Representation for the items provided by the given Iterator.
     * @param items The items to be written to the output. toString will be called on each item 
     * as it is written to the output.  Items will be separated by a newline character by default.
     */
    public StreamableTextRepresentation(Iterator<?> items) {
        this(items, MediaType.TEXT_PLAIN);
    }

    /**
     * Create a Representation for the items provided by the given Iterator.
     * @param items The items to be written to the output. toString will be called on each item 
     * as it is written to the output.  Items will be separated by a newline character by default.
     * @param mediaType The MediaType to be used for the output.
     */
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

    /**
     * Get the String which will be inserted between each output item. Returns 
     * System.getProperty("line.separator") by default.
     */
    protected String getItemSeparator() {
        return SEPARATOR;
    }

}
