package org.obd.ws.resources;

import org.apache.log4j.Logger;
import org.obd.query.Shard;
import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Reference;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.ResourceException;
import org.restlet.resource.Variant;

public class HomologyResource extends Resource {
    
    private final Shard shard;
    private final String termID;

    public HomologyResource(Context context, Request request, Response response) {
        super(context, request, response);
        this.shard = (Shard)this.getContext().getAttributes().get("shard");
        this.termID = Reference.decode((String)(request.getAttributes().get("termID")));
        this.getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    }

    @Override
    public Representation represent(Variant variant) throws ResourceException {
        // TODO Auto-generated method stub
        // the implementation needs to go here!
        return super.represent(variant);
    }
    
    private Logger log() {
        return Logger.getLogger(this.getClass());
    }

}
