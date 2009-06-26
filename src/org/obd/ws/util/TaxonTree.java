package org.obd.ws.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.obd.ws.util.dto.NodeDTO;

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
	
	private NodeDTO root;
	private Map<NodeDTO, Set<NodeDTO>> branchingPointsAndChildren;
	private Set<NodeDTO> leaves;
	/** This structure maps a taxon to the EQ combinations it is associated with */
	private Map<NodeDTO,  Map<NodeDTO, Set<NodeDTO>>> nodeToEQMap;
	/** This structure maps a taxon to the number of annotations associated with it */
	private Map<NodeDTO, Integer> nodeToAnnotationCountMap; 
	/** This structure maps a node to the Reif Ids that mention it*/
	private Map<NodeDTO, List<String>> nodeToReifIdsMap;
	/** This structure maps a taxon to the count of the entity associated with it */
	private Map<NodeDTO, Map<NodeDTO, Set<String>>> nodeToECMap;

	/*
	 * GETTERs and SETTERs
	 * 
	 */
	public NodeDTO getRoot() {
		return root;
	}
	public void setRoot(NodeDTO root) {
		this.root = root;
	}
	
	public Map<NodeDTO, Set<NodeDTO>> getBranchingPointsAndChildren() {
		return branchingPointsAndChildren;
	}
	public void setBranchingPointsAndChildren(
			Map<NodeDTO, Set<NodeDTO>> branchingPointsAndChidren) {
		this.branchingPointsAndChildren = branchingPointsAndChidren;
	}

	public Set<NodeDTO> getLeaves() {
		return leaves;
	}
	public void setLeaves(Set<NodeDTO> leaves) {
		this.leaves = leaves;
	}
	
	public Map<NodeDTO, Map<NodeDTO, Set<NodeDTO>>> getNodeToEQMap() {
		return nodeToEQMap;
	}
	public void setNodeToEQMap(
			Map<NodeDTO,  Map<NodeDTO, Set<NodeDTO>>> nodeToQualitiesMap) {
		this.nodeToEQMap = nodeToQualitiesMap;
	}
	
	public Map<NodeDTO, Integer> getNodeToAnnotationCountMap() {
		return nodeToAnnotationCountMap;
	}
	public void setNodeToAnnotationCountMap(
			Map<NodeDTO, Integer> nodeToAnnotationCountMap) {
		this.nodeToAnnotationCountMap = nodeToAnnotationCountMap;
	}

	public Map<NodeDTO, List<String>> getNodeToReifIdsMap() {
		return nodeToReifIdsMap;
	}
	public void setNodeToReifIdsMap(Map<NodeDTO, List<String>> nodeToReifIdsMap) {
		this.nodeToReifIdsMap = nodeToReifIdsMap;
	}
	
	public Map<NodeDTO, Map<NodeDTO, Set<String>>> getNodeToECMap() {
		return nodeToECMap;
	}
	public void setNodeToECMap(Map<NodeDTO, Map<NodeDTO, Set<String>>> nodeToECMap) {
		this.nodeToECMap = nodeToECMap;
	}
	/**
	 * The constructor simply initializes the branches, the leaves, the qualities to node
	 * map and the node to annotation count map
	 */
	public TaxonTree(){
		branchingPointsAndChildren = new HashMap<NodeDTO, Set<NodeDTO>>(); 
		leaves = new HashSet<NodeDTO>();
		nodeToEQMap = new HashMap<NodeDTO,  Map<NodeDTO, Set<NodeDTO>>>();
		nodeToAnnotationCountMap = new HashMap<NodeDTO, Integer>();
		nodeToReifIdsMap = new HashMap<NodeDTO, List<String>>();
		nodeToECMap = new HashMap<NodeDTO, Map<NodeDTO, Set<String>>>();
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
	public void printTaxonomy(NodeDTO node, int tabCt, BufferedWriter bw) 
		throws IOException{
		String tabs = "";

		for(int i = 0; i < tabCt; i++){
			tabs += "  ";
		}
		if(!this.getLeaves().contains(node)){//this is not a leaf node
			if(this.getBranchingPointsAndChildren().get(node) != null ){
				for(NodeDTO child : this.getBranchingPointsAndChildren().get(node)){
					bw.write(tabs + child + "\n");
					bw.write(tabs + "Annotation Count: " + this.getNodeToAnnotationCountMap().get(child) + "\n");
					if(this.getNodeToEQMap().containsKey(child)){
						Map<NodeDTO, Set<NodeDTO>> e2qMap = this.getNodeToEQMap().get(child);
						for(NodeDTO e : e2qMap.keySet()){
							bw.write(tabs + "Exhibits " + e.getName() + " that are: ");
							for(NodeDTO q : e2qMap.get(e)){
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
