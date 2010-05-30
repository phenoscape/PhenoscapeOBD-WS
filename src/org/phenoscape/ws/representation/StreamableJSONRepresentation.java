package org.phenoscape.ws.representation;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Iterator;

import org.json.JSONObject;
import org.restlet.data.MediaType;

public class StreamableJSONRepresentation extends StreamableTextRepresentation {

    private String key;
    private JSONObject otherValues = new JSONObject();
    private static final String SEPARATOR = "," + System.getProperty("line.separator"); 

    public StreamableJSONRepresentation(Iterator<? extends JSONObject> items) {
        super(items, MediaType.APPLICATION_JSON);
    }

    public StreamableJSONRepresentation(Iterator<? extends JSONObject> items, String key) {
        super(items, MediaType.APPLICATION_JSON);
    }

    public StreamableJSONRepresentation(Iterator<? extends JSONObject> items, String key, JSONObject otherValues) {
        super(items, MediaType.APPLICATION_JSON);
    }

    @Override
    public void write(OutputStream stream) throws IOException {
        final Writer writer = new BufferedWriter(new OutputStreamWriter(stream, this.getCharacterEncoding()));
        if (this.key != null) {
            final String json = this.otherValues.toString();
            final String openEndedJSON = json.substring(0, json.lastIndexOf("}"));
            writer.append(openEndedJSON);
            writer.append(JSONObject.quote(this.key));
            writer.append(":");
        }
        writer.write("[");
        super.write(stream);
        writer.write("]");
        if (this.key != null) { writer.write("}"); }
    }

    @Override
    protected String getItemSeparator() {
        return SEPARATOR;
    }

}
