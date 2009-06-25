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
 * This is a tree order for a group of taxa, with a root
 * (the MRCA of all the taxa), branches and leaves. At each node
 * in the tree, the grouping of qualities exhibited by the taxa
 * at and below that node are stored. So are the numbers of 
 * assertions for the taxon at the node and the taxa below that node   
 * @author cartik
 *
 */
public class TaxonTree {
	
	private NodeDTO root;
	private Set<NodeDTO> leaves;
	
	/** This structure keeps track of branching points and their children. 
	 * NOTE: The root  */
	private Map<NodeDTO, Set<NodeDTO>> parentToChildrenMap;

	/**This structure keeps track of all the EQs exhibited by a node and its descendants  */
	private Map<NodeDTO,  Map<NodeDTO, Set<NodeDTO>>> nodeToEQMap;
	/** This structure keeps track of annotation counts for the node and its descendants **/
	private Map<NodeDTO, Integer> nodeToAnnotationCountMap; 
	/** This has been added tp keep track of reif link ids - Cartik 061909*/
	private Map<NodeDTO, List<String>> nodeToReifIdsMap;

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
	
	public Set<NodeDTO> getLeaves() {
		return leaves;
	}
	public void setLeaves(Set<NodeDTO> leaves) {
		this.leaves = leaves;
	}
	
	public Map<NodeDTO, Set<NodeDTO>> getParentToChildrenMap() {
		return parentToChildrenMap;
	}
	public void setParentToChildrenMap(
			Map<NodeDTO, Set<NodeDTO>> parentToChildrenMap) {
		this.parentToChildrenMap = parentToChildrenMap;
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
	
	/**
	 * The constructor simply initializes the branches, the leaves, the qualities to node
	 * map, node to reif ids map and the node to annotation count map
	 */
	public TaxonTree(){
		parentToChildrenMap = new HashMap<NodeDTO, Set<NodeDTO>>(); 
		leaves = new HashSet<NodeDTO>();
		nodeToEQMap = new HashMap<NodeDTO,  Map<NodeDTO, Set<NodeDTO>>>();
		nodeToReifIdsMap = new HashMap<NodeDTO, List<String>>();
		nodeToAnnotationCountMap = new HashMap<NodeDTO, Integer>();
	}
	
	/**
	 * @PURPOSE This is a test method to print out the entire
	 * stored taxonomy with the proper indentation
	 * @CAUTION Hardcoded file path for writing out the taxonomy
	 * @param node - the node whose children are to printed along with the node
	 * itself
	 * @param tabCt - the number of indents. used for formatting the output text
	 * @param writer - buffered writer which contains a pointer to a text file
	 * @throws IOException
	 */
	public void printTaxonomy(NodeDTO node, int tabCt, BufferedWriter writer) 
		throws IOException{
		String tabs = "";

		for(int i = 0; i < tabCt; i++){
			tabs += "  ";
		}
		if(!this.getLeaves().contains(node)){//this is not a leaf node
			if(this.getParentToChildrenMap().get(node) != null ){
				for(NodeDTO child : this.getParentToChildrenMap().get(node)){
					writer.write(tabs + child.getId() + "\t" + child.getName() + "\n");
					writer.write(tabs + "Annotation Count: " + this.getNodeToAnnotationCountMap().get(child) + "\n");
					if(this.getNodeToEQMap().containsKey(child)){
						Map<NodeDTO, Set<NodeDTO>> eToQMap = this.getNodeToEQMap().get(child);
						for(NodeDTO e : eToQMap.keySet()){
							writer.write(tabs + "Exhibits " + e.getName() + " that are: ");
							for(NodeDTO q : eToQMap.get(e)){
								writer.write(q.getName() + " "); 
							}
							writer.write("\n");
						}
					}
					
					printTaxonomy(child, tabCt + 1, writer);
				}
			}
		}
		else{
			return;
		}
	}
}
