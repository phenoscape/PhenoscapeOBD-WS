package org.obd.ws.application;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.obd.query.impl.OBDSQLShard;
import org.restlet.Application;
import org.restlet.Context;
import org.restlet.Restlet;
import org.restlet.Router;

public class OBDApplication extends Application {


    public OBDApplication(Context context){
        super(context);
    }

    private void connect() throws SQLException, ClassNotFoundException, IOException{
        OBDSQLShard obdsql = new OBDSQLShard();
        InputStream fis = this.getClass().getResourceAsStream("connectionInfo.properties");
        Properties props = new Properties(); 
        props.load(fis);
        String dbHost = (String)props.get("dbHost");
        String uid = (String)props.get("uid");
        String pwd = (String)props.get("pwd");

        //			String localDbHost = (String)props.get("localDbHost");
        //			String localUid = (String)props.get("localUid");
        //			String localPwd = (String)props.get("localPwd");

        obdsql.connect(dbHost, uid, pwd);
        this.getContext().getAttributes().put("shard", obdsql);
    }

    public Restlet createRoot() {
        try {
            connect();
        } catch (SQLException e) {
            log().fatal("Error connecting to SQL shard", e);
        } catch (ClassNotFoundException e) {
            log().fatal("Error creating SQL shard", e);
        } catch (IOException e) {
            log().fatal("Error reading connection properties file", e);
        }
        final Router router = new Router(this.getContext());
        // URL mappings
        router.attach("/phenotypes", org.obd.ws.resources.PhenotypeDetailsResource.class);
        router.attach("/phenotypes/summary", org.obd.ws.resources.PhenotypeSummaryResource.class);
        router.attach("/term/search", org.obd.ws.resources.AutoCompleteResource.class);
        router.attach("/term/{termID}", org.obd.ws.resources.TermResource.class);
        router.attach("/term/{termID}/homology", org.obd.ws.resources.HomologyResource.class);
        return router;
    }
    
    private Logger log() {
        return Logger.getLogger(this.getClass());
    }

}
