package org.obd.ws.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.obd.ws.util.dto.NodeDTO;
import org.obd.ws.util.dto.PhenotypeDTO;

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
	
	private NodeDTO mrca;
	private Set<NodeDTO> leaves;
	/** This structure maps a taxon to the EQC combinations it is associated with. We also track the
	 * reif ids associated with each EQC combination */
	private Map<NodeDTO,  List<PhenotypeDTO>> nodeToListOfEQCRListsMap;
	/** This structure maps a taxon to the number of annotations associated with it */
	private Map<NodeDTO, Integer> nodeToAnnotationCountMap; 
	/** This structure hold information about every branch and its children */
	private Map<NodeDTO, Set<NodeDTO>> nodeToChildrenMap;
	/** This structure maps every node to the set of leaf taxa under it */
	private Map<NodeDTO, Set<NodeDTO>> nodeToSubsumedLeafNodesMap;

	/*
	 * GETTERs and SETTERs
	 * 
	 */
	public NodeDTO getMrca() {
		return mrca;
	}
	public void setMrca(NodeDTO mrca) {
		this.mrca = mrca;
	}
	
	public Set<NodeDTO> getLeaves() {
		return leaves;
	}
	public void setLeaves(Set<NodeDTO> leaves) {
		this.leaves = leaves;
	}
	
	public Map<NodeDTO, List<PhenotypeDTO>> getNodeToListOfEQCRListsMap() {
		return nodeToListOfEQCRListsMap;
	}
	public void setNodeToListOfEQCRListsMap(
			Map<NodeDTO,  List<PhenotypeDTO>> nodeToListOfEQCRListsMap) {
		this.nodeToListOfEQCRListsMap = nodeToListOfEQCRListsMap;
	}
	
	public Map<NodeDTO, Integer> getNodeToAnnotationCountMap() {
		return nodeToAnnotationCountMap;
	}
	public void setNodeToAnnotationCountMap(
			Map<NodeDTO, Integer> nodeToAnnotationCountMap) {
		this.nodeToAnnotationCountMap = nodeToAnnotationCountMap;
	}
	
	public Map<NodeDTO, Set<NodeDTO>> getNodeToChildrenMap() {
		return nodeToChildrenMap;
	}
	public void setNodeToChildrenMap(Map<NodeDTO, Set<NodeDTO>> nodeToChildrenMap) {
		this.nodeToChildrenMap = nodeToChildrenMap;
	}
	
	public Map<NodeDTO, Set<NodeDTO>> getNodeToSubsumedLeafNodesMap() {
		return nodeToSubsumedLeafNodesMap;
	}
	public void setNodeToSubsumedLeafNodesMap(
			Map<NodeDTO, Set<NodeDTO>> nodeToSubsumedLeafNodesMap) {
		this.nodeToSubsumedLeafNodesMap = nodeToSubsumedLeafNodesMap;
	}
	/**
	 * The constructor simply initializes the branches, the leaves, the qualities to node
	 * map and the node to annotation count map
	 */
	public TaxonTree(){
		leaves = new HashSet<NodeDTO>();
		nodeToListOfEQCRListsMap = new HashMap<NodeDTO,  List<PhenotypeDTO>>();
		nodeToAnnotationCountMap = new HashMap<NodeDTO, Integer>();
		nodeToChildrenMap = new HashMap<NodeDTO, Set<NodeDTO>>();
		nodeToSubsumedLeafNodesMap = new HashMap<NodeDTO, Set<NodeDTO>>();
	}
}
