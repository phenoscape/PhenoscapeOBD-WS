package org.obd.ws.util;

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
	private Set<NodeDTO> leaves;
	/** This structure maps a taxon to the EQC combinations it is associated with. We also track the
	 * reif ids associated with each EQC combination */
	private Map<NodeDTO,  List<List<NodeDTO>>> nodeToListOfEQCRListsMap;
	/** This structure maps a taxon to the number of annotations associated with it */
	private Map<NodeDTO, Integer> nodeToAnnotationCountMap; 

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
	
	public Map<NodeDTO, List<List<NodeDTO>>> getNodeToListOfEQCRListsMap() {
		return nodeToListOfEQCRListsMap;
	}
	public void setNodeToListOfEQCRListsMap(
			Map<NodeDTO,  List<List<NodeDTO>>> nodeToListOfEQCRListsMap) {
		this.nodeToListOfEQCRListsMap = nodeToListOfEQCRListsMap;
	}
	
	public Map<NodeDTO, Integer> getNodeToAnnotationCountMap() {
		return nodeToAnnotationCountMap;
	}
	public void setNodeToAnnotationCountMap(
			Map<NodeDTO, Integer> nodeToAnnotationCountMap) {
		this.nodeToAnnotationCountMap = nodeToAnnotationCountMap;
	}

	
	/**
	 * The constructor simply initializes the branches, the leaves, the qualities to node
	 * map and the node to annotation count map
	 */
	public TaxonTree(){
		leaves = new HashSet<NodeDTO>();
		nodeToListOfEQCRListsMap = new HashMap<NodeDTO,  List<List<NodeDTO>>>();
		nodeToAnnotationCountMap = new HashMap<NodeDTO, Integer>();
	}
}
