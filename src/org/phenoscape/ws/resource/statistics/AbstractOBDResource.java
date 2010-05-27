package org.phenoscape.ws.resource.statistics;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.obd.query.impl.OBDSQLShard;
import org.phenoscape.ws.application.PhenoscapeWebServiceApplication;
import org.restlet.resource.ResourceException;
import org.restlet.resource.ServerResource;

public abstract class AbstractOBDResource extends ServerResource {

    private OBDSQLShard shard;

    /* (non-Javadoc)
     * @see org.restlet.resource.UniformResource#doInit()
     * If a subclass overrides this method, it should call super's before its own implementation.
     */
    @Override
    protected void doInit() throws ResourceException {
        super.doInit();
        this.shard = new OBDSQLShard();
    }

    protected OBDSQLShard getShard() {
        return this.shard;
    }

    /**
     * This method reads in db connection parameters from app context before connecting
     * the Shard to the database
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    protected void connectShardToDatabase() throws SQLException, ClassNotFoundException{
        this.shard.connect((DataSource)(this.getContext().getAttributes().get(PhenoscapeWebServiceApplication.DATA_SOURCE_KEY)));
    }

    protected void disconnectShardFromDatabase() {
        this.shard.disconnect();
    }

    protected Logger log() {
        return Logger.getLogger(this.getClass());
    }

}
