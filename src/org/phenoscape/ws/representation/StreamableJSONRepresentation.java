package org.phenoscape.ws.representation;

import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;

import org.json.JSONObject;
import org.restlet.data.CharacterSet;
import org.restlet.data.MediaType;

public class StreamableJSONRepresentation extends StreamableTextRepresentation {

    private final String key;
    private final JSONObject otherValues;
    private static final String SEPARATOR = "," + System.getProperty("line.separator"); 

    public StreamableJSONRepresentation(Iterator<? extends JSONObject> items) {
        this(items, null);
    }

    public StreamableJSONRepresentation(Iterator<? extends JSONObject> items, String key) {
        this(items, key, new JSONObject());
    }

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

    @Override
    protected String getItemSeparator() {
        return SEPARATOR;
    }

}
