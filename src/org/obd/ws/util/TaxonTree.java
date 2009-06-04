package org.obd.ws.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.obo.datamodel.OBOClass;

public class TaxonTree {
	
	/**
	 * A tree consists of a root and the branching
	 * points, and all the paths from the root
	 * and the branching points to the leaves
	 */
	private OBOClass root;
	private Map<OBOClass, Set<OBOClass>> branchingPointsAndChidren;
	private List<List<OBOClass>> paths;

	/*
	 * GETTERs and SETTERs
	 * 
	 */
	
	public List<List<OBOClass>> getPaths() {
		return paths;
	}
	public void setPaths(List<List<OBOClass>> paths) {
		this.paths = paths;
	}
	
	public OBOClass getRoot() {
		return root;
	}
	public void setRoot(OBOClass root) {
		this.root = root;
	}
	
	public Map<OBOClass, Set<OBOClass>> getBranchingPointsAndChidren() {
		return branchingPointsAndChidren;
	}
	public void setBranchingPointsAndChidren(
			Map<OBOClass, Set<OBOClass>> branchingPointsAndChidren) {
		this.branchingPointsAndChidren = branchingPointsAndChidren;
	}
	
	public TaxonTree(){
		paths = new ArrayList<List<OBOClass>>();
		root = null;
		branchingPointsAndChidren = new HashMap<OBOClass, Set<OBOClass>>(); 
	}
	
}
