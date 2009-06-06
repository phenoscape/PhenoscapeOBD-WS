package org.obd.ws.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.obo.datamodel.OBOClass;

public class TaxonTree {
	
	/**
	 * A tree consists of a root and the branching
	 * points and the children of the branching points
	 * and the leaves as well
	 */
	private OBOClass root;
	private Map<OBOClass, Set<OBOClass>> branchingPointsAndChildren;
	private Set<OBOClass> leaves;

	/*
	 * GETTERs and SETTERs
	 * 
	 */
	public OBOClass getRoot() {
		return root;
	}
	public void setRoot(OBOClass root) {
		this.root = root;
	}
	
	public Map<OBOClass, Set<OBOClass>> getBranchingPointsAndChildren() {
		return branchingPointsAndChildren;
	}
	public void setBranchingPointsAndChildren(
			Map<OBOClass, Set<OBOClass>> branchingPointsAndChidren) {
		this.branchingPointsAndChildren = branchingPointsAndChidren;
	}

	public Set<OBOClass> getLeaves() {
		return leaves;
	}
	public void setLeaves(Set<OBOClass> leaves) {
		this.leaves = leaves;
	}
	/**
	 * The constructor simply initializes the branches and the leaves
	 */
	public TaxonTree(){
		branchingPointsAndChildren = new HashMap<OBOClass, Set<OBOClass>>(); 
		leaves = new HashSet<OBOClass>();
	}
	
	/**
	 * @PURPOSE This is a test method to print out the entire
	 * stored taxonomy with the proper indentation
	 * @CAUTION Hardcoded file path for writing out the taxonomy
	 * @param node - the node whose children are to printed along with the node
	 * itself
	 * @param tabCt - the number of indents
	 * @param bw - buffered reader which contains a pointer to a text file
	 * @throws IOException
	 */
	public void printTaxonomy(OBOClass node, int tabCt, BufferedWriter bw) 
		throws IOException{
		String tabs = "";

		for(int i = 0; i < tabCt; i++){
			tabs += "  ";
		}
		if(!this.getLeaves().contains(node)){//this is not a leaf node
			if(this.getBranchingPointsAndChildren().get(node) != null ){
				for(OBOClass child : this.getBranchingPointsAndChildren().get(node)){
					bw.write(tabs + child.getID() + "\t" + child.getName() + "\n");
					printTaxonomy(child, tabCt + 1, bw);
				}
			}
		}
		else{
			return;
		}
	}
}
