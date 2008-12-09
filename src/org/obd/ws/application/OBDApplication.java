package org.obd.ws.application;

import java.sql.SQLException;

import org.obd.query.Shard;
import org.obd.query.impl.OBDSQLShard;
import org.obd.query.impl.AbstractSQLShard;
import org.restlet.Application;
import org.restlet.Context;
import org.restlet.Restlet;
import org.restlet.Router;

public class OBDApplication extends Application {

	public OBDApplication(Context context){
		super(context);
	}
	
	public void connect() throws SQLException, ClassNotFoundException{
		Shard obdsql = new OBDSQLShard();
		((AbstractSQLShard)obdsql).connect("j?", 
				"?", "?");
		this.getContext().getAttributes().put("shard", obdsql);
	}
	
    public Restlet createRoot() {
    	try{
    		connect();
    	}
    	catch(Exception e){
    		e.printStackTrace();
    	}
    	
        final Router router = new Router(this.getContext());
        // URL mappings
       // router.attachDefault(HelloWorldResource.class);
        
        router.attach("/term/search", org.obd.ws.resources.AutoCompleteResource.class);
        router.attach("/term/{termID}", org.obd.ws.resources.TermResource.class);

        return router;
    }

}
