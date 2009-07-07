package org.obd.ws.application;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.obd.query.impl.OBDSQLShard;

/**
 * This class has been written to toggle the connection 
 * between databases, selecting the one which has been updated 
 * most recently
 * @author cartik
 */
public class DatabaseToggler {
	
	private final String driverName = "jdbc:postgresql://"; 
	/**
	 * The names of the two databases. These can be changed
	 */
	private String dbName1 = "obdphenoscape";
	private String dbName2 = "obdphenoscape_test";
	
	/**
	 * This method chooses the newer database and returns the Shard connecting to this
	 * database
	 * @return the Shard connection to the newer of the two databases
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws ParseException
	 */
	public OBDSQLShard chooseDatabase() throws SQLException, ClassNotFoundException, 
					IOException, ParseException{
        OBDSQLShard obdsql1 = new OBDSQLShard();
        OBDSQLShard obdsql2 = new OBDSQLShard();
        
        InputStream fis = this.getClass().getResourceAsStream("connectionInfo.properties");
        Properties props = new Properties(); 
        props.load(fis);

        String dbHost = (String)props.get("dbHost");
        String db1 = driverName + dbHost + "/" + dbName1;
        String db2 = driverName + dbHost + "/" + dbName2;
        String uid = (String)props.get("uid");
        String pwd = (String)props.get("pwd");
        

        //			String localDbHost = (String)props.get("localDbHost");
        //			String localUid = (String)props.get("localUid");
        //			String localPwd = (String)props.get("localPwd");

        obdsql1.connect(db1, uid, pwd);
        obdsql2.connect(db2, uid, pwd);
        
        Connection conn1 = obdsql1.getConnection();
        Connection conn2 = obdsql2.getConnection();
        
        String sqlQuery = "SELECT notes FROM obd_schema_metadata";
        
        Statement query1 = conn1.createStatement();
        Statement query2 = conn2.createStatement();
        
        ResultSet rs1 = query1.executeQuery(sqlQuery);
        ResultSet rs2 = query2.executeQuery(sqlQuery);
        
        String timeStamp1 = null, timeStamp2 = null; 
        
        while(rs1.next()){
        	timeStamp1 = rs1.getString(1);
        }
        while(rs2.next()){
        	timeStamp2 = rs2.getString(1);
        }
        
        if (timeStamp1 == null && timeStamp2 != null){
        	return obdsql2;
        }else if (timeStamp1 != null && timeStamp2 == null){
        	return obdsql1;
		}else if (timeStamp1 == null && timeStamp2 == null){
			return null;
		}else{
			String[] dateNtime1 = timeStamp1.split("\\_");
			String[] dateNtime2 = timeStamp2.split("\\_");
        
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        
        	Date date1 = sdf.parse(dateNtime1[0]);
        	Date date2 = sdf.parse(dateNtime2[0]);
        	
			Time time1 = Time.valueOf(dateNtime1[1]);
			Time time2 = Time.valueOf(dateNtime2[1]);
			
			if(!date1.equals(date2))
				return date1.after(date2)?obdsql1 : obdsql2;
			else
				if(!time1.equals(time2))
					return time1.after(time2)?obdsql1 : obdsql2;
				else return obdsql2;
		}
    }
	
	/**
	 * Main method has been created to test this class
	 * @param args
	 * @throws ParseException
	 * @throws IOException
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws ParseException, 
		IOException, SQLException, ClassNotFoundException{
		DatabaseToggler dt = new DatabaseToggler();
		OBDSQLShard shard = dt.chooseDatabase();
		Connection conn = shard.getConnection();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT notes FROM obd_schema_metadata");
		rs.next();
		System.out.println(rs.getString(1));
	}
}
