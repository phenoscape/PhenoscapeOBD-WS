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
		((AbstractSQLShard)obdsql).connect("?", 
				"?", "?");
		//((AbstractSQLShard)obdsql).connect("jdbc:postgresql://localhost:5433/obdtest121308", 
		//		"?", "?");
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
        router.attach("/phenotypes/summary/{termID}", org.obd.ws.resources.AnatomyResource.class);
        router.attach("/phenotypes/anatomy/{termID}/taxa/{patoID}", org.obd.ws.resources.TaxaForAnatomyResource.class);
        router.attach("/phenotypes/anatomy/{termID}/genes/{patoID}", org.obd.ws.resources.GenesForAnatomyResource.class);
        router.attach("/phenotypes/summary/gene/{termID}", org.obd.ws.resources.GeneResource.class);
        
        return router;
    }

}
