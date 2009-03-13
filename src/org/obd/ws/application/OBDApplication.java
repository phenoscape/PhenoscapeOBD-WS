package org.obd.ws.application;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.obd.query.Shard;
import org.obd.query.impl.AbstractSQLShard;
import org.obd.query.impl.OBDSQLShard;
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
		
		
		try{
			InputStream fis = this.getClass().getResourceAsStream("connectionInfo.properties");
			Properties props = new Properties(); 
			props.load(fis);
			String dbHost = (String)props.get("dbHost");
			String uid = (String)props.get("uid");
			String pwd = (String)props.get("pwd");
			
//			String localDbHost = (String)props.get("localDbHost");
//			String localUid = (String)props.get("localUid");
//			String localPwd = (String)props.get("localPwd");
			
			((AbstractSQLShard)obdsql).connect(dbHost, 
					uid, pwd);
			
			String driverName = "org.postgresql.Driver";
			Class.forName(driverName);
			Connection conn = DriverManager.getConnection(dbHost, uid, pwd);
			//((AbstractSQLShard)obdsql).connect(localDbHost, 
			//		localUid, localPwd);
			
			this.getContext().getAttributes().put("shard", obdsql);
			this.getContext().getAttributes().put("conn", conn);
		}
		catch(Exception e){
			e.printStackTrace();
		}
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
        router.attach("/phenotypes/summary/anatomy/{termID}", org.obd.ws.resources.AnatomyResource.class);
        router.attach("/phenotypes/anatomy/{termID}/taxa/{patoID}", org.obd.ws.resources.TaxaForAnatomyResource.class);
        router.attach("/phenotypes/anatomy/{termID}/genes/{patoID}", org.obd.ws.resources.GenesForAnatomyResource.class);
        router.attach("/phenotypes/summary/gene/{termID}", org.obd.ws.resources.GeneResource.class);
        
        return router;
    }

}
