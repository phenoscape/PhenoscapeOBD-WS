package org.obd.ws.util;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bbop.dataadapter.DataAdapterException;
import org.bbop.util.ObjectUtil;
import org.obd.ws.util.dto.NodeDTO;
import org.obd.ws.util.dto.PhenotypeDTO;

/**
 * @author cartik
 * The purpose of this class is to create a tree of annotations
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
	 * This constructor takes in a list of phenotypes and 
	 * generates a Taxon Tree, with annotations at each node 
	 * of the tree. It uses the Teleost Taxonomy {@link TTOTaxonomy} 
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
	 * This method takes in a list of taxa and returns their MRCA
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
	 * A helper method to find the MRCA of any two given taxa.
	 * this is an overloaded method
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
	 * A utility method to return the intersection of two paths
	 * @param pathFromOclass - path from one taxon to the root
	 * @param pathFromMrca - path from the other txon to the root
	 * @return - the intersection node of the two paths
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
	 * @return a path from the given node to the MRCA of the tree
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
	 * @throws IOException
	 * @throws DataAdapterException
	 */
	private void constructTreeFromPhenotypeDTOs() throws IOException, DataAdapterException{
		for(PhenotypeDTO phenotype : phenotypeColl){
			updateTreeNodesWithPhenotype(phenotype);
			updateTreeNodesWithAnnotationCounts(phenotype);
			updateTreeNodesWithSubsumedLeafTaxa(phenotype);
			updateTreeBranches(phenotype);
		}
	}
	
	/**
	 * This method updates the phenotypes of all the ancestor nodes 
	 * of the taxon of the input assertion
	 * @param phenotype - the input taxon to phenotype assertion
	 */
	private void updateTreeNodesWithPhenotype(PhenotypeDTO phenotype){
		
		Map<NodeDTO, List<List<String>>> nodeToListOfEQCRListsMap = 
										tree.getNodeToListOfEQCRListsMap();
		
		NodeDTO taxon = new NodeDTO(phenotype.getTaxonId()) ;
		taxon.setName(phenotype.getTaxon());
		
		tree.getLeaves().add(taxon);

		List<NodeDTO> pathToMrca = getPathToMrca(taxon);
		for(NodeDTO node : pathToMrca){
			List<List<String>> listOfEQCRLists = 
				nodeToListOfEQCRListsMap.get(node);
			
			if(listOfEQCRLists == null)
				listOfEQCRLists = addPhenotypeToListOfEQCRLists(phenotype, listOfEQCRLists, 
						(pathToMrca.indexOf(node) == 0));
			else{
				listOfEQCRLists = consolidateReifIds(phenotype, listOfEQCRLists, (pathToMrca.indexOf(node) == 0));
			}
			nodeToListOfEQCRListsMap.put(node, listOfEQCRLists);
		}
		tree.setNodeToListOfEQCRListsMap(nodeToListOfEQCRListsMap);
	}
	/**
	 * This method is used to consolidate reif ids at nodes, which already have the
	 * input phenotype. 
	 * @param phenotype - the input phenotype
	 * @param listOfEqcrLists - the existing set of phenotype annotations for the node
	 * @param isLeafNode - a boolean that tells if the node that is being handled has the phenotype
	 * directly asserted to it if TRUE, or if FALSE, it is a higher node in the taxonomy 
	 * @return an EQCR list with the reif ids consolidated in a comma delimited list
	 */
	private List<List<String>> consolidateReifIds(PhenotypeDTO phenotype, List<List<String>> listOfEqcrLists, 
			boolean isLeafNode){
		for(int i = 0 ; i < listOfEqcrLists.size(); i++){
			List<String> eqcrList = listOfEqcrLists.get(i);
			if(isPhenotypeSameAsEqcrList(phenotype, eqcrList)){
				if(isLeafNode){
					String rFromEqcr = eqcrList.remove(5);
					String rFromPhenotype = phenotype.getReifId();
					rFromEqcr += "," + rFromPhenotype;
					eqcrList.add(5, rFromEqcr);
				}
				else{
					eqcrList.remove(5);
					eqcrList.add(5, "");
				}
				listOfEqcrLists.remove(i);
				listOfEqcrLists.add(i, eqcrList);
				return listOfEqcrLists;
			}
		}
		return addPhenotypeToListOfEQCRLists(phenotype, listOfEqcrLists, isLeafNode);
	}
	
	/**
	 * A method to check if the input phenotype matches an eqcr representation;
	 * specifically, the Entity Quality Character of the EQCR. The R (Reif Id) is not
	 * considered
	 * @param phenotype - the input phenotype
	 * @param eqcrList - the eqcr List against which the input phenotype is compared
	 * @return a boolean to indicate a match between the input phenotype and the eqcr List
	 */
	private boolean isPhenotypeSameAsEqcrList(PhenotypeDTO phenotype, List<String> eqcrList){
		String e = eqcrList.get(0);
		String q = eqcrList.get(2);
		String c = eqcrList.get(4);
		String relEnt = eqcrList.get(6);
		
		String eFromPhenotype = phenotype.getEntityId();
		String qFromPhenotype = phenotype.getQualityId();
		String cFromPhenotype = phenotype.getNumericalCount();
		String relEntFromPhenotype = phenotype.getRelatedEntityId();
		if (e.equals(eFromPhenotype) && q.equals(qFromPhenotype) && ObjectUtil.equals(c, cFromPhenotype)) {
			if(relEnt != null && relEntFromPhenotype != null){
				if(relEnt.equals(relEntFromPhenotype))
					return true;
				else
					return false;
			}
		    return true;
		}
		return false;
	}
	
	/**
	 * This method adds the new phenotype to the existing loist of EQCRs associated with the node
	 * @param phenotype - the input phenotype
	 * @param listOfEQCRLists - the existing eqcr lists for the node
	 * @param isLeafNode - a boolean that tells if the node that is being handled has the phenotype
	 * directly asserted to it if TRUE, or if FALSE, it is a higher node in the taxonomy  
	 * @return a list of EQCR lists
	 */
	private List<List<String>> addPhenotypeToListOfEQCRLists(PhenotypeDTO phenotype, List<List<String>> listOfEQCRLists, 
			boolean isLeafNode){
		if(listOfEQCRLists == null)
			listOfEQCRLists = new LinkedList<List<String>>();
		List<String> eqcrList = new LinkedList<String>();
		eqcrList.add(phenotype.getEntityId());
		eqcrList.add(phenotype.getEntity());
		eqcrList.add(phenotype.getQualityId());
		eqcrList.add(phenotype.getQuality());
		eqcrList.add(phenotype.getNumericalCount());
		if(isLeafNode){
			eqcrList.add(phenotype.getReifId());
		}else{
			eqcrList.add("");
		}
		eqcrList.add(phenotype.getRelatedEntityId());
		eqcrList.add(phenotype.getRelatedEntity());	
		listOfEQCRLists.add(eqcrList);
		return listOfEQCRLists;
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
	 * This method updates every node in the tree with the set of leaf nodes
	 * sp. the set of taxa with assertions, that are subsumed by it 
	 * @param phenotype - the input taxon to phenotype assertion
	 */
	private void updateTreeNodesWithSubsumedLeafTaxa(PhenotypeDTO phenotype) {
		Map<NodeDTO, Set<NodeDTO>> taxonToSubsumedTaxaMap = 
			tree.getNodeToSubsumedLeafNodesMap();
		
		NodeDTO taxon = new NodeDTO(phenotype.getTaxonId());
		taxon.setName(phenotype.getTaxon());
		
		List<NodeDTO> pathToMrca = getPathToMrca(taxon);
		for(NodeDTO node : pathToMrca){
			Set<NodeDTO> setOfSubsumedTaxa = taxonToSubsumedTaxaMap.get(node);
			if(setOfSubsumedTaxa == null)
				setOfSubsumedTaxa = new HashSet<NodeDTO>();
			setOfSubsumedTaxa.add(taxon);
			taxonToSubsumedTaxaMap.put(node, setOfSubsumedTaxa);
		}
		tree.setNodeToSubsumedLeafNodesMap(taxonToSubsumedTaxaMap);
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
