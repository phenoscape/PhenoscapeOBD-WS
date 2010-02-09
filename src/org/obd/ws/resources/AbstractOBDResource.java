package org.obd.ws.resources;

import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.obd.query.impl.OBDSQLShard;
import org.obd.ws.application.OBDApplication;
import org.restlet.Context;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.resource.Resource;

public abstract class AbstractOBDResource extends Resource {

    private static final String DRIVER_NAME = "jdbc:postgresql://";
    protected OBDSQLShard shard;

    public AbstractOBDResource(Context context, Request request, Response response) {
        super(context, request, response);
        try {
            this.shard = new OBDSQLShard();
        } catch (SQLException e) {
            log().fatal("Failed to create shard", e);
        } catch (ClassNotFoundException e) {
            log().fatal("Failed to create shard", e);
        }
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
        String dbName = (String)this.getContext().getAttributes().get(OBDApplication.SELECTED_DATABASE_NAME_STRING);
        String dbHost = (String)this.getContext().getAttributes().get(OBDApplication.DB_HOST_NAME_STRING);
        String uid = (String)this.getContext().getAttributes().get(OBDApplication.UID_STRING);
        String pwd = (String)this.getContext().getAttributes().get(OBDApplication.PWD_STRING);
        String dbConnString = DRIVER_NAME + dbHost + "/" + dbName;
        this.shard.connect(dbConnString, uid, pwd);
    }

    protected void disconnectShardFromDatabase() {
        this.shard.disconnect();
    }

    protected Logger log() {
        return Logger.getLogger(this.getClass());
    }

}
