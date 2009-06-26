package org.obd.ws.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.bbop.dataadapter.DataAdapterException;
import org.obd.ws.util.dto.NodeDTO;
import org.obd.ws.util.dto.PhenotypeDTO;

/**
 * @author cartik
 * @PURPOSE The purpose of this class is to recreate the hierarchy of 
 * teleost species in a data structure. In addition, it includes a method
 * to determine the Most Recent Common Ancestor (MRCA) of any set of taxa 
 */

public class TaxonomyBuilder {
	
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

	/**
	 * The default constructor reads in the ontology from the 
	 * URL and stores it in a file. It also intializes the
	 * instance variables
	 * @throws IOException
	 * @throws DataAdapterException 
	 */
	public TaxonomyBuilder(TTOTaxonomy ttoTaxonomy, Collection<PhenotypeDTO> phenotypeColl) 
					throws IOException, DataAdapterException{
		this.ttoTaxonomy = ttoTaxonomy;
		this.taxonColl = new LinkedList<NodeDTO>();
		this.phenotypeColl = phenotypeColl;
		generateTaxonListFromPhenotypeList();
	}
	
	/**
	 * A simple method that extracts the taxon from each
	 * phenotype and creates a new collection of these 
	 * taxa
	 */
	public void generateTaxonListFromPhenotypeList(){
		NodeDTO taxon;
		for(PhenotypeDTO phenotype : phenotypeColl){
			taxon = new NodeDTO(phenotype.getTaxonId());
			taxon.setName(phenotype.getTaxon());
			taxonColl.add(taxon);
		}
	}
	
	/**
	 * @PURPOSE This method takes in a list of taxa and returns their MRCA
	 * @param taxa - the list of taxa whose MRCA is to be determined
	 * @param mrca - the current MRCA
	 * @return the MRCA of the entire list of taxa
	 */
	public NodeDTO findMRCA(LinkedList<NodeDTO> taxa, NodeDTO mrca){
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
	public NodeDTO findMRCA(NodeDTO oClass, NodeDTO mrca){
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
	 * Constructs a taxon tree from the input list of 
	 * phenotype DTOs, complete with EQ annotations for
	 * every node, annotation count, and reif ids
	 * @return
	 * @throws IOException
	 * @throws DataAdapterException
	 */
	public TaxonTree constructTreeFromPhenotypeDTOs() throws IOException, DataAdapterException{
		
		TaxonTree tree = new TaxonTree();
		Map<NodeDTO, List<List<NodeDTO>>> taxonToListOfEQCRListsMap = 
					tree.getNodeToListOfEQCRListsMap();
		Map<NodeDTO, Integer> taxonToAnnotationCountMap = 
					tree.getNodeToAnnotationCountMap();
		NodeDTO taxon, entity, quality, count, reifId;
		for(PhenotypeDTO phenotype : phenotypeColl){
			taxon = new NodeDTO(phenotype.getTaxonId());
			taxon.setName(phenotype.getTaxon());
			entity = new NodeDTO(phenotype.getEntityId());
			entity.setName(phenotype.getEntity());
			quality = new NodeDTO(phenotype.getQualityId());
			quality.setName(phenotype.getQuality());
			count = new NodeDTO(phenotype.getNumericalCount());
			count.setName("");
			reifId = new NodeDTO(phenotype.getReifId());
			reifId.setName("");
			
			//Now we want to update everything for the upstream taxa all the way to the root
			for(NodeDTO node : ttoTaxonomy.getNodeToPathMap().get(taxon)){
				//handle EQ annotations here
				//taxonToEQMap = assignEQAnnotationToTaxon(dto, taxonToEQMap);
				List<List<NodeDTO>> listOfEQCRLists = taxonToListOfEQCRListsMap.get(node);
				if(listOfEQCRLists == null){
					listOfEQCRLists = new ArrayList<List<NodeDTO>>();
				}
				List<NodeDTO> eqcrList = Arrays.asList(new NodeDTO[]{entity, quality, count, reifId});
				listOfEQCRLists.add(eqcrList);
				
				taxonToListOfEQCRListsMap.put(node, listOfEQCRLists);
				
				//handle annotation counts here
				//taxonToAnnotationCountMap = assignAnnotationCountToTaxon(dto, taxonToAnnotationCountMap);
				Integer annotationCtForTaxon = taxonToAnnotationCountMap.get(node);
				if(annotationCtForTaxon == null)
					annotationCtForTaxon = 0;
				taxonToAnnotationCountMap.put(node, ++annotationCtForTaxon);				
			}
		}
		tree.setNodeToListOfEQCRListsMap(taxonToListOfEQCRListsMap);
		tree.setNodeToAnnotationCountMap(taxonToAnnotationCountMap);
		
		return tree;
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
