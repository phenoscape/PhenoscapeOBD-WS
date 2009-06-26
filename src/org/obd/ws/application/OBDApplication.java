package org.obd.ws.application;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;

import org.apache.log4j.Logger;
import org.bbop.dataadapter.DataAdapterException;
import org.obd.query.impl.OBDSQLShard;
import org.obd.ws.exceptions.PhenoscapeDbConnectionException;
import org.obd.ws.util.TTOTaxonomy;
import org.obd.ws.util.TeleostTaxonomyBuilder;
import org.restlet.Application;
import org.restlet.Context;
import org.restlet.Restlet;
import org.restlet.Router;

public class OBDApplication extends Application {


    public OBDApplication(Context context){
        super(context);
    }

    private void connect() throws SQLException, ClassNotFoundException, IOException, ParseException, 
    				PhenoscapeDbConnectionException, DataAdapterException{
    	
    	DatabaseToggler dbToggler = new DatabaseToggler();
    	
        OBDSQLShard obdsql = dbToggler.chooseDatabase();
        if(obdsql != null)
        	this.getContext().getAttributes().put("shard", obdsql);
        else
        	throw new PhenoscapeDbConnectionException("Failed to obtain a connection to the database. " +
        			"This is because neither database is ready to be queried. ");
        
        TTOTaxonomy ttoTaxonomy = new TTOTaxonomy();
        this.getContext().getAttributes().put("ttoTaxonomy", ttoTaxonomy);
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
        } catch (ParseException e) {
        	log().fatal("Error parsing the date", e);
        } catch (PhenoscapeDbConnectionException e) {
        	log().fatal("Error with the database connection", e);
        } catch (DataAdapterException e) {
        	log().fatal("Error reading in the OBO files", e);
		}
        
        final Router router = new Router(this.getContext());
        // URL mappings
        router.attach("/phenotypes", org.obd.ws.resources.PhenotypeDetailsResource.class);
        router.attach("/phenotypes/summary", org.obd.ws.resources.PhenotypeSummaryResource.class);
        router.attach("/phenotypes/source/{annotation_id}", org.obd.ws.resources.AnnotationResource.class);
        router.attach("/term/search", org.obd.ws.resources.AutoCompleteResource.class);
        router.attach("/term/{termID}", org.obd.ws.resources.TermResource.class);
        router.attach("/term/{termID}/homology", org.obd.ws.resources.HomologyResource.class);
        return router;
    }
    
    private Logger log() {
        return Logger.getLogger(this.getClass());
    }

}
