package org.phenoscape.ws.representation;

import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;

import org.json.JSONObject;
import org.restlet.data.CharacterSet;
import org.restlet.data.MediaType;

/**
 * A Representation which can be used for very large datasets without requiring that the full dataset is 
 * in memory all at once.  The provided Iterator can be implemented to retrieve data items on demand. The 
 * data items are represented as a root-level JSON array by default, but the array can be wrapped in an outer 
 * JSON object if a key is provided.   
 * @author Jim Balhoff
 */
public class StreamableJSONRepresentation extends StreamableTextRepresentation {

    private final String key;
    private final JSONObject otherValues;
    private static final String SEPARATOR = "," + System.getProperty("line.separator"); 

    /**
     * Create a Representation for the JSON objects provided by the given Iterator.
     * @param items The JSON objects to be written to the output. toString will be called on each item 
     * as it is written to the output, which will be in the form of a JSON array.
     */
    public StreamableJSONRepresentation(Iterator<? extends JSONObject> items) {
        this(items, null);
    }

    /**
     * Create a Representation for the JSON objects provided by the given Iterator.
     * @param items The JSON objects to be written to the output. toString will be called on each item 
     * as it is written to the output.
     * @param key If a key is provided, the output will be a JSON object with the JSON array 
     * representing the data items included as the value for this key.
     */
    public StreamableJSONRepresentation(Iterator<? extends JSONObject> items, String key) {
        this(items, key, new JSONObject());
    }

    /**
     * Create a Representation for the JSON objects provided by the given Iterator.
     * @param items The JSON objects to be written to the output. toString will be called on each item 
     * as it is written to the output.
     * @param key If a key is provided, the output will be a JSON object with the JSON array 
     * representing the data items included as the value for this key.
     * @param otherValues A JSON object containing other keys and values which should be included in the output 
     * JSON object. These keys and values will be written before the key containing the JSON array of data items. 
     * Any data in this JSON object will not benefit from the streaming used for the iterated data items. 
     */
    public StreamableJSONRepresentation(Iterator<? extends JSONObject> items, String key, JSONObject otherValues) {
        super(items, MediaType.APPLICATION_JSON);
        this.key = key;
        this.otherValues = otherValues;
        this.setCharacterSet(CharacterSet.UTF_8); //this should be the default for JSON
    }

    @Override
    public void write(Writer writer) throws IOException {
        if (this.key != null) {
            final String json = this.otherValues.toString();
            final String openEndedJSON = json.substring(0, json.lastIndexOf("}"));
            writer.append(openEndedJSON);
            writer.append(JSONObject.quote(this.key));
            writer.append(":");
        }
        writer.write("[");
        super.write(writer);
        writer.write("]");
        if (this.key != null) { writer.write("}"); }
    }

    /**
     * Returns a separator appropriate for JSON array syntax - a comma and newline by default. 
     */
    @Override
    protected String getItemSeparator() {
        return SEPARATOR;
    }

}
