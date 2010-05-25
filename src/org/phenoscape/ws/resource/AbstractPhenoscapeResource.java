package org.phenoscape.ws.resource;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.phenoscape.obd.model.PhenoscapeDataStore;
import org.phenoscape.ws.application.PhenoscapeWebServiceApplication;
import org.restlet.data.Reference;
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
     * Return first value of the given query parameter, decoded, or null if not present;
     */
    protected String getFirstQueryValue(String parameter) {
        if (this.getQuery().getFirstValue(parameter) != null) {
            return Reference.decode(this.getQuery().getFirstValue(parameter));
        } else {
            return null;
        }
    }
    
    protected boolean getBooleanQueryValue(String parameter, boolean defaultValue) {
        if (this.getQuery().getFirstValue(parameter) != null) {
            final String queryValue = this.getFirstQueryValue(parameter);
            return Boolean.parseBoolean(queryValue);
        } else {
            return defaultValue;
        }
    }
    
    protected int getIntegerQueryValue(String parameter, int defaultValue) {
        if (this.getQuery().getFirstValue(parameter) != null) {
            final String queryValue = this.getFirstQueryValue(parameter);
            return Integer.parseInt(queryValue);
        } else {
            return defaultValue;
        }
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
