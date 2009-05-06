package org.obd.ws.application;

import java.io.InputStream;
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
			
			this.getContext().getAttributes().put("shard", obdsql);
			
			InputStream queriesFile = this.getClass().getResourceAsStream("queries.properties");
			Properties queries = new Properties();
			queries.load(queriesFile);
			
			String aq = queries.getProperty("anatomyQuery");
			String tq = queries.getProperty("taxonQuery");
			String gq = queries.getProperty("geneQuery");
			String sgq = queries.getProperty("simpleGeneQuery");
			
			this.getContext().getAttributes().put("anatomyQuery", aq);
			this.getContext().getAttributes().put("taxonQuery", tq);
			this.getContext().getAttributes().put("geneQuery", gq);
			this.getContext().getAttributes().put("simpleGeneQuery", sgq);
			
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
        router.attach("/phenotypes", org.obd.ws.resources.PhenotypeDetailsResource.class);
        router.attach("/phenotypes/summary", org.obd.ws.resources.PhenotypeSummaryResource.class);
        router.attach("/term/search", org.obd.ws.resources.AutoCompleteResource.class);
        router.attach("/term/{termID}", org.obd.ws.resources.TermResource.class);        
        return router;
    }

}
