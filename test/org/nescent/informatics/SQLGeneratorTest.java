package org.nescent.informatics;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.obd.model.LinkStatement;

public class SQLGeneratorTest {

	@Test
	public void testConstructPaths() {
		
		
		
		LinkStatement ls1 = new LinkStatement("taxon", "exhibits", "phenotype");
		LinkStatement ls2 = new LinkStatement("gene", "hasAllele", "taxon");
		LinkStatement ls3 = new LinkStatement("state", "valueFor", "character");
		LinkStatement ls4 = new LinkStatement("phenotype", "inheresIn", "anatomy");
		LinkStatement ls5 = new LinkStatement("phenotype", "isA", "state");
		
		List<LinkStatement> lsList = new ArrayList<LinkStatement>();
		lsList.add(ls1);
		lsList.add(ls2);
		lsList.add(ls3);
		lsList.add(ls4);
		lsList.add(ls5);
		
		SQLGenerator sqlg = new SQLGenerator(lsList);
		
		for(String path : sqlg.constructPaths(null, lsList)){
			System.out.println(path);
		}
	}

	@Test
	public void testConstructSQLQuery() {
		fail("Not yet implemented");
	}

}
