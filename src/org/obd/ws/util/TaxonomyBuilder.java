package org.obd.ws.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bbop.dataadapter.DataAdapterException;
import org.obd.ws.util.dto.NodeDTO;
import org.obd.ws.util.dto.PhenotypeDTO;

/**
 * @author cartik
 * @PURPOSE The purpose of this class is to create a tree of annotations
 * from an input of phenotypes. The tree is rooted at the MRCA of the 
 * taxa from the input phenotypes
 */

public class TaxonomyBuilder {
	
	private TaxonTree tree;
	private TTOTaxonomy ttoTaxonomy;
	private Collection<PhenotypeDTO> phenotypeColl;
	private LinkedList<NodeDTO> taxonColl;
	
	/*GETTERs */
	public Collection<PhenotypeDTO> getPhenotypeColl() {
		return phenotypeColl;
	}

	public LinkedList<NodeDTO> getTaxonColl() {
		return taxonColl;
	}
	
	public TaxonTree getTree(){
		return tree;
	}
	/**
	 * @PURPOSE This constructor takes in a list of phenotypes and 
	 * generates a Taxon Tree, with annotations at each node 
	 * of the tree. It uses the Teleost Taxonomy (TTOTaxonomy)
	 * class for this purpose
	 * @param ttoTaxonomy  taxonomically arranged TTO nodes
	 * @param phenotypeColl input phenotypes
	 * @throws IOException
	 * @throws DataAdapterException
	 */
	public TaxonomyBuilder(TTOTaxonomy ttoTaxonomy, Collection<PhenotypeDTO> phenotypeColl) 
					throws IOException, DataAdapterException{
		this.tree = new TaxonTree();
		this.ttoTaxonomy = ttoTaxonomy;
		this.taxonColl = new LinkedList<NodeDTO>();
		this.phenotypeColl = phenotypeColl;
		generateTaxonListFromPhenotypeList();
		NodeDTO mrca = this.findMRCA(taxonColl, null);
		tree.setMrca(mrca);
		constructTreeFromPhenotypeDTOs();
	}
	
	/**
	 * A simple method that extracts the taxon from each
	 * phenotype and creates a new collection of these 
	 * taxa
	 */
	private void generateTaxonListFromPhenotypeList(){
		NodeDTO taxon;
		Set<NodeDTO> setOfTaxa = new HashSet<NodeDTO>();
		for(PhenotypeDTO phenotype : phenotypeColl){
			taxon = new NodeDTO(phenotype.getTaxonId());
			taxon.setName(phenotype.getTaxon());
			setOfTaxa.add(taxon);
		}
		taxonColl.addAll(setOfTaxa);
	}
	
	/**
	 * @PURPOSE This method takes in a list of taxa and returns their MRCA
	 * @param taxa - the list of taxa whose MRCA is to be determined
	 * @param mrca - the current MRCA
	 * @return the MRCA of the entire list of taxa
	 */
	private NodeDTO findMRCA(LinkedList<NodeDTO> taxa, NodeDTO mrca){
		if(taxa.size() == 0 || ttoTaxonomy.getRoot().equals(mrca))
			return mrca;
		NodeDTO first = taxa.remove(0);
		NodeDTO newMrca = findMRCA(first, mrca); 
		return findMRCA(taxa, newMrca);
	}
	
	/**
	 * @PURPOSE A helper method to find the MRCA of any two given taxa.
	 * @NOTE this is an overloaded method and is private access only
	 * @param oClass - one of the two taxa
	 * @param mrca - the other taxon
	 * @return - the mrca of the two taxa
	 */
	private NodeDTO findMRCA(NodeDTO oClass, NodeDTO mrca){
		if(mrca == null)
			return oClass;
		else if(ttoTaxonomy.getNodeToPathMap().get(oClass).contains(mrca))
			return mrca;
		else if(ttoTaxonomy.getNodeToPathMap().get(mrca).contains(oClass))
			return oClass;
		else{
			List<NodeDTO> pathFromOclass = ttoTaxonomy.getNodeToPathMap().get(oClass);
			List<NodeDTO> pathFromMrca = ttoTaxonomy.getNodeToPathMap().get(mrca);
			return findBranchingPointInPaths(pathFromOclass, pathFromMrca);
		}
	}
	
	/**
	 * @PURPOSE A utility method to return the intersection of two paths
	 * @param pathFromOclass - path from one taxon to the root
	 * @param pathFromMrca - path from the other txon to the root
	 * @return - the intersection of the two paths
	 */
	private NodeDTO findBranchingPointInPaths(List<NodeDTO> pathFromOclass,
			List<NodeDTO> pathFromMrca) {
		for(NodeDTO taxon : pathFromMrca){
			if(pathFromOclass.contains(taxon)){
				return taxon;
			}
		}
		return pathFromMrca.get(0);
	}
	/**
	 * A method to trace the path from the input node to
	 * the MRCA of the tree
	 * @param nodeDTO
	 * @return
	 */
	private List<NodeDTO> getPathToMrca(NodeDTO nodeDTO){
		List<NodeDTO> pathToTTORoot = 
			ttoTaxonomy.getNodeToPathMap().get(nodeDTO);
		int indexOfMrca = pathToTTORoot.indexOf(tree.getMrca());
		return pathToTTORoot.subList(0, indexOfMrca + 1);
	}
	
	/**
	 * Constructs a taxon tree from the input list of 
	 * phenotype DTOs, complete with EQ annotations for
	 * every node, annotation count, and reif ids
	 * @return
	 * @throws IOException
	 * @throws DataAdapterException
	 */
	private void constructTreeFromPhenotypeDTOs() throws IOException, DataAdapterException{
		for(PhenotypeDTO phenotype : phenotypeColl){
			updateTreeNodesWithPhenotype(phenotype);
			updateTreeNodesWithAnnotationCounts(phenotype);
			updateTreeBranches(phenotype);
		}
	}
	
	/**
	 * @PURPOSE This method updates the phenotypes of all the ancestor nodes 
	 * of the taxon of the input assertion
	 * @param phenotype - the input taxon to phenotype assertion
	 */
	private void updateTreeNodesWithPhenotype(PhenotypeDTO phenotype){
		
		Map<NodeDTO, List<List<String>>> nodeToListOfEQCRListsMap = 
										tree.getNodeToListOfEQCRListsMap();
		
		NodeDTO taxon = new NodeDTO(phenotype.getTaxonId()) ;
		taxon.setName(phenotype.getTaxon());
		
		tree.getLeaves().add(taxon);
		
		String count = phenotype.getNumericalCount();
		if(count == null)
			count = "";
		List<NodeDTO> pathToMrca = getPathToMrca(taxon);
		for(NodeDTO node : pathToMrca){
			List<List<String>> listOfEQCRLists = 
				nodeToListOfEQCRListsMap.get(node);
			if(listOfEQCRLists == null)
				listOfEQCRLists = new ArrayList<List<String>>();
			listOfEQCRLists.add(Arrays.asList(new String[]{
				phenotype.getEntityId(),
				phenotype.getEntity(),
				phenotype.getQualityId(),
				phenotype.getQuality(),
				phenotype.getNumericalCount(),
				phenotype.getReifId()
			}));
			nodeToListOfEQCRListsMap.put(node, listOfEQCRLists);
		}
		tree.setNodeToListOfEQCRListsMap(nodeToListOfEQCRListsMap);
	}
	
	/**
	 * This method updates the annotation counts of all the ancestor nodes of 
	 * the taxon from the input assertion 
	 * @param phenotype - the input taxon to phenotype assertion
	 */
	private void updateTreeNodesWithAnnotationCounts(PhenotypeDTO phenotype){
		Map<NodeDTO, Integer> taxonToAnnotationCountMap = 
			tree.getNodeToAnnotationCountMap();
		
		NodeDTO taxon = new NodeDTO(phenotype.getTaxonId()) ;
		taxon.setName(phenotype.getTaxon());
		List<NodeDTO> pathToMrca = getPathToMrca(taxon);
		for(NodeDTO node : pathToMrca){
			Integer annotationCtForTaxon = taxonToAnnotationCountMap.get(node);
			if(annotationCtForTaxon == null)
				annotationCtForTaxon = 0;
			taxonToAnnotationCountMap.put(node, ++annotationCtForTaxon);	
		}
		tree.setNodeToAnnotationCountMap(taxonToAnnotationCountMap);
	}
	
	/**
	 * This method creates a map of branching points to
	 * children from the input taxon to phenotype assertion
	 * @param phenotype - the taxon to phenotype assertion
	 */
	private void updateTreeBranches(PhenotypeDTO phenotype){
		Map<NodeDTO, Set<NodeDTO>> nodeToChildrenMap = 
								tree.getNodeToChildrenMap();
		NodeDTO taxon = new NodeDTO(phenotype.getTaxonId()) ;
		taxon.setName(phenotype.getTaxon());
		List<NodeDTO> pathToMrca = getPathToMrca(taxon);
		for(int i = 0;  i < pathToMrca.size() - 1; i++){
			NodeDTO node = pathToMrca.get(i);
			NodeDTO parent = pathToMrca.get(i + 1);
			Set<NodeDTO> children = nodeToChildrenMap.get(parent);
			if(children == null)
				children = new HashSet<NodeDTO>();
			children.add(node);
			nodeToChildrenMap.put(parent, children);
		}
		tree.setNodeToChildrenMap(nodeToChildrenMap);
	}
	
	/**
	 * The main method. This will be used to test the other methods before
	 * they are invoked directly from the other classes REST resources to
	 * be specific 
	 * @param args
	 * @throws DataAdapterException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws DataAdapterException, IOException{

	}
}
