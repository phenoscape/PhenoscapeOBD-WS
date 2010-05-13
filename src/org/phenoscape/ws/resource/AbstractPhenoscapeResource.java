package org.phenoscape.ws.resource;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.phenoscape.obd.query.PhenoscapeDataStore;
import org.phenoscape.ws.application.PhenoscapeWebServiceApplication;
import org.restlet.resource.ServerResource;

/**
 * An abstract Restlet resource providing functionality needed by any resource, such as creation of query objects and database connections.
 * @author Jim Balhoff
 */
public class AbstractPhenoscapeResource extends ServerResource {
    
    private PhenoscapeDataStore dataStore = null;
    
    /**
     * Get an instance of PhenoscapeDataStore initialized with this application's JDBC connection.
     */
    protected PhenoscapeDataStore getDataStore() {
        if (this.dataStore == null) {
            this.dataStore = new PhenoscapeDataStore(this.getDataSource());
        }
        return this.dataStore;
    }
    
    /**
     * Retrieve the JDBC DataSource from the application context.
     */
    private DataSource getDataSource() {
        return (DataSource)(this.getContext().getAttributes().get(PhenoscapeWebServiceApplication.DATA_SOURCE_KEY));
    }
    
    protected Logger log() {
        return Logger.getLogger(this.getClass());
    }

}
