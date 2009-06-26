package org.obd.ws.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
 * @PURPOSE The purpose of this class is to recreate the hierarchy of 
 * teleost species in a data structure. In addition, it includes a method
 * to determine the Most Recent Common Ancestor (MRCA) of any set of taxa 
 */

public class TeleostTaxonomyBuilder {
	
	TTOTaxonomy ttoTaxnmy;
	Collection<PhenotypeDTO> phenotypeColl;
	List<NodeDTO> taxonColl;
	
	/**
	 * The default constructor reads in the ontology from the 
	 * URL and stores it in a file. It also intializes the
	 * instance variables
	 * @throws IOException
	 * @throws DataAdapterException 
	 */
	public TeleostTaxonomyBuilder(TTOTaxonomy ttoTaxnmy, Collection<PhenotypeDTO> phenotypeColl) 
					throws IOException, DataAdapterException{
		this.ttoTaxnmy = ttoTaxnmy;
		this.taxonColl = new LinkedList<NodeDTO>();
		this.phenotypeColl = phenotypeColl;
		generateTaxonsFromPhenotypes();
	}
	
	public void generateTaxonsFromPhenotypes(){
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
		if(taxa.size() == 0 || ttoTaxnmy.getRoot().equals(mrca))
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
		else if(ttoTaxnmy.getNodeToPathMap().get(oClass).contains(mrca))
			return mrca;
		else if(ttoTaxnmy.getNodeToPathMap().get(mrca).contains(oClass))
			return oClass;
		else{
			List<NodeDTO> pathFromOclass = ttoTaxnmy.getNodeToPathMap().get(oClass);
			List<NodeDTO> pathFromMrca = ttoTaxnmy.getNodeToPathMap().get(mrca);
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
	
	public TaxonTree constructTreeFromPhenotypeDTOs() throws IOException, DataAdapterException{
		
		TaxonTree tree = new TaxonTree();
		Map<NodeDTO, Map<NodeDTO, Set<NodeDTO>>> taxonToEQMap = 
					tree.getNodeToEQMap();
		Map<NodeDTO, Integer> taxonToAnnotationCountMap = 
					tree.getNodeToAnnotationCountMap();
		Map<NodeDTO, List<String>> taxonToReifIdsMap = 
					tree.getNodeToReifIdsMap();
		NodeDTO taxon, entity, quality;
		String reifId;
		for(PhenotypeDTO phenotype : phenotypeColl){
			taxon = new NodeDTO(phenotype.getTaxonId());
			taxon.setName(phenotype.getTaxon());
			entity = new NodeDTO(phenotype.getEntityId());
			entity.setName(phenotype.getEntity());
			quality = new NodeDTO(phenotype.getQualityId());
			quality.setName(phenotype.getQuality());
			reifId = phenotype.getReifId();
			
			//Now we want to update everything for the upstream taxa all the way to the root
			for(NodeDTO node : ttoTaxnmy.getNodeToPathMap().get(taxon)){
				//handle EQ annotations here
				//taxonToEQMap = assignEQAnnotationToTaxon(dto, taxonToEQMap);
				Map<NodeDTO, Set<NodeDTO>> entityToQualitySetMap = taxonToEQMap.get(node);
				if(entityToQualitySetMap == null){
					entityToQualitySetMap = new HashMap<NodeDTO, Set<NodeDTO>>();
				}
				Set<NodeDTO> qualitySet = entityToQualitySetMap.get(entity);
				if(qualitySet == null)
					qualitySet = new HashSet<NodeDTO>();
				qualitySet.add(quality);
				entityToQualitySetMap.put(entity, qualitySet);
				taxonToEQMap.put(node, entityToQualitySetMap);
				
				//handle annotation counts here
				//taxonToAnnotationCountMap = assignAnnotationCountToTaxon(dto, taxonToAnnotationCountMap);
				Integer annotationCtForTaxon = taxonToAnnotationCountMap.get(node);
				if(annotationCtForTaxon == null)
					annotationCtForTaxon = 0;
				taxonToAnnotationCountMap.put(node, ++annotationCtForTaxon);
				
				//handle reif ids here
				//taxonToReifIdsMap = assignReifIdsToTaxon(dto, taxonToReifIdsMap);
				List<String> reifIdsForTaxon = taxonToReifIdsMap.get(node);
				if(reifIdsForTaxon == null)
					reifIdsForTaxon = new ArrayList<String>();
				reifIdsForTaxon.add(reifId);
				taxonToReifIdsMap.put(node, reifIdsForTaxon);
			}
		}
		tree.setNodeToEQMap(taxonToEQMap);
		tree.setNodeToAnnotationCountMap(taxonToAnnotationCountMap);
		tree.setNodeToReifIdsMap(taxonToReifIdsMap);
		
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
