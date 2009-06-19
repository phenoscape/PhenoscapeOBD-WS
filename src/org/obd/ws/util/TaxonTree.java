package org.obd.ws.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.obo.datamodel.OBOClass;

/**
 * This is a tree order for a group on taxa, with a root
 * (the MRCA of all the taxa), branches and leaves. At each node
 * in the tree, the grouping of qualities exhibited by the taxa
 * at and below that node are stored. So are the numbers of 
 * assertions for the taxon at the node and the taxa below that node   
 * @author cartik
 *
 */
public class TaxonTree {
	
	private OBOClass root;
	private Map<OBOClass, Set<OBOClass>> branchingPointsAndChildren;
	private Set<OBOClass> leaves;
	
	private Map<OBOClass,  Map<OBOClass, Set<OBOClass>>> nodeToEQMap;
	private Map<OBOClass, Integer> nodeToAnnotationCountMap; 
	/** This has been added tp keep track of reif link ids - Cartik 061909*/
	private Map<OBOClass, String[]> nodeToAnnotationMap;

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
	
	public Map<OBOClass, Map<OBOClass, Set<OBOClass>>> getNodeToEQMap() {
		return nodeToEQMap;
	}
	public void setNodeToEQMap(
			Map<OBOClass,  Map<OBOClass, Set<OBOClass>>> nodeToQualitiesMap) {
		this.nodeToEQMap = nodeToQualitiesMap;
	}
	
	public Map<OBOClass, Integer> getNodeToAnnotationCountMap() {
		return nodeToAnnotationCountMap;
	}
	public void setNodeToAnnotationCountMap(
			Map<OBOClass, Integer> nodeToAnnotationCountMap) {
		this.nodeToAnnotationCountMap = nodeToAnnotationCountMap;
	}

	public Map<OBOClass, String[]> getNodeToAnnotationMap() {
		return nodeToAnnotationMap;
	}
	public void setNodeToAnnotationMap(Map<OBOClass, String[]> nodeToAnnotationMap) {
		this.nodeToAnnotationMap = nodeToAnnotationMap;
	}
	/**
	 * The constructor simply initializes the branches, the leaves, the qualities to node
	 * map and the node to annotation count map
	 */
	public TaxonTree(){
		branchingPointsAndChildren = new HashMap<OBOClass, Set<OBOClass>>(); 
		leaves = new HashSet<OBOClass>();
		nodeToEQMap = new HashMap<OBOClass,  Map<OBOClass, Set<OBOClass>>>();
		nodeToAnnotationCountMap = new HashMap<OBOClass, Integer>();
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
					bw.write(tabs + "Annotation Count: " + this.getNodeToAnnotationCountMap().get(child) + "\n");
					if(this.getNodeToEQMap().containsKey(child)){
						Map<OBOClass, Set<OBOClass>> e2qMap = this.getNodeToEQMap().get(child);
						for(OBOClass e : e2qMap.keySet()){
							bw.write(tabs + "Exhibits " + e.getName() + " that are: ");
							for(OBOClass q : e2qMap.get(e)){
								bw.write(q.getName() + " "); 
							}
							bw.write("\n");
						}
					}
					
					printTaxonomy(child, tabCt + 1, bw);
				}
			}
		}
		else{
			return;
		}
	}
}
