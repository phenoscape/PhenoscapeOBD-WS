package org.obd.ws.application.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.junit.Test;
import org.obd.query.Shard;
import org.obd.query.impl.AbstractSQLShard;
import org.obd.query.impl.OBDSQLShard;
import org.obd.ws.resources.AutoCompleteResource;
import org.obd.ws.resources.TermResource;
import org.restlet.resource.Representation;

public class OBDApplicationTest {

	@Test
	public void testTermSearch(){
		try {
			File connParamFile = new File("testfiles/connectionParameters");
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
			TermResource tr = new TermResource(obdsql, "ZFA:0000107");
			Representation rep2 = tr.getRepresentation(tr.getVariants().get(0));
			AutoCompleteResource acr = new AutoCompleteResource(obdsql, "basihyal", new String[]{"true", "true", "true", "TTO:TAO:COLLECTION"});
			Representation rep = acr.getRepresentation(acr.getVariants().get(0));
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
