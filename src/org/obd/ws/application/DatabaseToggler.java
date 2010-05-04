package org.obd.ws.application;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Properties;

import org.obd.query.impl.OBDSQLShard;

/**
 * This class has been written to toggle the connection 
 * between databases, selecting the one which has been updated 
 * most recently
 * @author cartik
 */
public class DatabaseToggler {

    private String uid;
    private String pwd;
    private String dbHost;	
    private String dbName;
    private static final String DRIVER_NAME = "jdbc:postgresql://";

    public String getUid() {
        return uid;
    }
    public String getPwd() {
        return pwd;
    }
    public String getDbHost() {
        return dbHost;
    }

    public String getDBName() {
        return this.dbName;
    }

    public OBDSQLShard chooseDatabase() throws SQLException, ClassNotFoundException, IOException, ParseException {
        final OBDSQLShard obdsql = new OBDSQLShard();
        final InputStream fis = this.getClass().getResourceAsStream("connectionInfo.properties");
        final Properties props = new Properties(); 
        props.load(fis);
        this.dbHost = (String)props.get("dbHost");
        this.dbName = (String)props.get("dbName");
        this.uid = (String)props.get("uid");
        this.pwd = (String)props.get("pwd");
        //TODO verify whether this needs to connect here
        final String connectionString = DRIVER_NAME + this.dbHost + "/" + this.dbName;
        obdsql.connect(connectionString, this.uid, this.pwd);
        return obdsql;
    }

}
