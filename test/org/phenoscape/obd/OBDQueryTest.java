package org.phenoscape.obd;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.obd.model.Node;
import org.obd.model.Statement;
import org.obd.query.Shard;
import org.obd.query.impl.AbstractSQLShard;
import org.obd.query.impl.OBDSQLShard;

public class OBDQueryTest {

	public Logger log = Logger.getLogger(this.getClass());
	/*
	 * @Test public void testConnectToDatabase() { fail("Not yet implemented");
	 * try{ obdq.connectToDatabase(); } catch(Exception e){ e.printStackTrace();
	 * } }
	 * 
	 * @Test public void testGetStatementsAboutNexusMatrix() {
	 * fail("Implemented but skipping over");
	 * obdq.getStatementsAboutNexusMatrix();
	 * 
	 * }
	 */
	@Test
	public void testQueryTerm(){
		try {
			InputStream connParamFile = new FileInputStream("connectionInfo/connectionInfo.properties");
			Properties props = new Properties();
			props.load(connParamFile);

			String dbHost = (String)props.get("dbHost");
			String uid = (String)props.get("uid");
			String pwd = (String)props.get("pwd");
			
			
//			String localDbHost = (String)props.get("localDbHost");
//			String localUid = (String)props.get("localUid");
//			String localPwd = (String)props.get("localPwd");
			
			Shard obdsql = new OBDSQLShard();
			((AbstractSQLShard) obdsql).connect(dbHost, uid, pwd);
//			((AbstractSQLShard) obdsql).connect(localDbHost, localUid, localPwd);
	
			
			InputStream queriesFile = new FileInputStream("connectionInfo/queries.properties");
			Properties queries = new Properties();
			queries.load(queriesFile);

			String aq = (String)queries.get("anatomyQuery");
			String tq = (String)queries.get("taxonQuery");
			String gq = (String)queries.get("geneQuery");
			String sgq = (String)queries.get("simpleGeneQuery");
			
			OBDQuery obdq = new OBDQuery(obdsql, new String[]{aq, tq, gq, sgq});
			
			
			Map<String, String> nodeProps, filterOptions = new HashMap<String, String>();
			String relId, target, character = null, taxon = null, state = null, entity = null;
			long testStartTime = System.currentTimeMillis(); 
			for(Node node : obdq.executeQueryAndAssembleResults(obdq.getAnatomyQuery(), "TAO:0000108", filterOptions)){				
				nodeProps = new HashMap<String, String>();
				
				for(Statement stmt : node.getStatements()){
					relId = stmt.getRelationId();
					target = stmt.getTargetId();
					nodeProps.put(relId, target);
				} 
				nodeProps.put("id", node.getId());
				character = nodeProps.get("hasCharacter");
				taxon = nodeProps.get("exhibitedBy");
				state = nodeProps.get("hasState");
				entity = nodeProps.get("inheresIn");
				
			
				System.out.println(taxon + " exhibits " + state + " " + character +  " in " + entity);
				
			}
			
			long testEndTime = System.currentTimeMillis();
			System.out.println("Time taken = " + (testEndTime - testStartTime) + " milliseconds");
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/*
	@Test
	public void testGenericSearch() {
		try {
			File connParamFile = new File("testfiles/dbConnectInfo");
			BufferedReader br = new BufferedReader(
					new FileReader(connParamFile));
			String[] connParams = new String[3];
			String param;
			int j = 0;
			while ((param = br.readLine()) != null) {
				connParams[j++] = param;
			}

			Shard obdsql = new OBDSQLShard();
			((AbstractSQLShard) obdsql).connect(connParams[0], connParams[1],
					connParams[2]);
			OBDQuery obdq = new OBDQuery(obdsql);
			Set<Statement> stmts = obdq.genericSearch("TTO:1001979", null,
					null);
			int i = 0;
			for (Iterator<Statement> it = stmts.iterator(); it.hasNext();) {
				Statement entry = it.next();

				System.out.println(++i + ". " + entry);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	*/
}
