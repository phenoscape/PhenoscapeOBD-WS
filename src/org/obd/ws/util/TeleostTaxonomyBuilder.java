package org.obd.ws.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.bbop.dataadapter.DataAdapterException;
import org.obd.ws.util.dto.NodeDTO;
import org.obo.dataadapter.OBOAdapter;
import org.obo.dataadapter.OBOFileAdapter;
import org.obo.datamodel.Link;
import org.obo.datamodel.OBOClass;
import org.obo.datamodel.OBOSession;
import org.obo.util.TermUtil;

/**
 * @author cartik
 * @PURPOSE The purpose of this class is to recreate the hierarchy of 
 * teleost species in a data structure. In addition, it includes a method
 * to determine the Most Recent Common Ancestor (MRCA) of any set of taxa 
 */

public class TeleostTaxonomyBuilder {
	
	private Map<NodeDTO, Set<NodeDTO>> parentToChildrenMap;
	private Map<NodeDTO, NodeDTO> childToParentMap;

	private Set<NodeDTO> roots;
	private Set<NodeDTO> leaves;
	
	private Map<String, NodeDTO> idToClassMapper; 

	/*
	 * GETTER methods for instance variables
	 */
	public Set<NodeDTO> getRoots() {
		return roots;
	}

	public Set<NodeDTO> getLeaves() {
		return leaves;
	}
	
	public Map<NodeDTO, Set<NodeDTO>> getParentToChildrenMap() {
		return parentToChildrenMap;
	}
	
	public Map<NodeDTO, NodeDTO> getChildToParentMap(){
		return childToParentMap;
	}
	
	public Map<String, NodeDTO> getIdToClassMapper(){
		return idToClassMapper;
	}
	
	/**
	 * The default constructor reads in the ontology from the 
	 * URL and stores it in a file. It also intializes the
	 * instance variables
	 * @throws IOException
	 * @throws DataAdapterException 
	 */
	public TeleostTaxonomyBuilder() throws IOException, DataAdapterException{
		parentToChildrenMap = new HashMap<NodeDTO, Set<NodeDTO>>();
		childToParentMap = new HashMap<NodeDTO, NodeDTO>();
		roots = new HashSet<NodeDTO>();
		leaves = new HashSet<NodeDTO>();
		idToClassMapper = new HashMap<String, NodeDTO>();
		
		URL ttoURL = new URL("http://obo.cvs.sourceforge.net/*checkout*/obo/obo/ontology/taxonomy/teleost_taxonomy.obo");
		URL taoURL = new URL("http://obo.cvs.sourceforge.net/*checkout*/obo/obo/ontology/anatomy/gross_anatomy/animal_gross_anatomy/fish/teleost_anatomy.obo");
		URL patoURL = new URL("http://obo.cvs.sourceforge.net/*checkout*/obo/obo/ontology/phenotype/quality.obo");
		BufferedReader br1 = new BufferedReader(new InputStreamReader(ttoURL.openStream()));
		BufferedReader br2 = new BufferedReader(new InputStreamReader(taoURL.openStream()));
		BufferedReader br3 = new BufferedReader(new InputStreamReader(patoURL.openStream()));
		
		String line; 
		File ttoFile = new File("teleost_taxonomy.obo");
		File taoFile = new File("teleost_anatomy.obo");
		File patoFile = new File("quality.obo");
		
		BufferedWriter pw1 = new BufferedWriter(new FileWriter(ttoFile));
		
		while((line = br1.readLine()) != null){
			pw1.write(line + "\n");
		}
		
		pw1.flush();
		pw1.close();
		
		BufferedWriter pw2 = new BufferedWriter(new FileWriter(taoFile));
		
		while((line = br2.readLine()) != null){
			pw2.write(line + "\n");
		}
		
		pw2.flush();
		pw2.close();
		
		BufferedWriter pw3 = new BufferedWriter(new FileWriter(patoFile));
		
		while((line = br3.readLine()) != null){
			pw3.write(line + "\n");
		}
		
		pw3.flush();
		pw3.close();
		
		setUpTaxonomy();
	}
	
	/**
	 * This method reads in the TTO, TAO and PATO 
	 * and arranges the terms in a taxonomy 
	 * In addition, it also adds all the terms to 
	 * a map from ID to the OBOClass
	 * @throws DataAdapterException 
	 * @throws IOException 
	 */
	public void setUpTaxonomy() throws DataAdapterException, IOException{
		
		List<String> paths = new ArrayList<String>();
		paths.add("teleost_taxonomy.obo");
		paths.add("teleost_anatomy.obo");
		paths.add("quality.obo");
		
		//setting up the file adapter and its configuration
		final OBOFileAdapter fileAdapter = new OBOFileAdapter();
		OBOFileAdapter.OBOAdapterConfiguration config = new OBOFileAdapter.OBOAdapterConfiguration();
	    config.setReadPaths(paths);
	    config.setBasicSave(false);
	    config.setAllowDangling(true);
	    config.setFollowImports(false);
	    
	    //read in the terms into the session
	    OBOSession session = 
	    	fileAdapter.doOperation(OBOAdapter.READ_ONTOLOGY, config, null);
	    	    
	    for(OBOClass oboClass : TermUtil.getTerms(session)){
	    	if(oboClass.getNamespace() != null && !oboClass.isObsolete() &&
	    			(oboClass.getID().matches("TTO:[0-9]+") ||
   					 oboClass.getID().matches("PATO:[0-9]+") ||
   					oboClass.getID().matches("TAO:[0-9]+"))){
	    		String id = oboClass.getID();
	    		String name = oboClass.getName();
	    		
	    		NodeDTO oboNode = new NodeDTO(id);
	    		oboNode.setName(name);
	    		idToClassMapper.put(oboClass.getID(), oboNode);
	    		
	    		if(oboClass.getChildren().size() == 0){//a leaf node
	    			leaves.add(oboNode);
	    			addNodeToParent(oboClass);
	    		}
	    		else if(oboClass.getParents().size() == 0){//a root node
	    			roots.add(oboNode);
	    			for(Link child : oboClass.getChildren()){
	    				OBOClass childclass = (OBOClass)child.getChild();
	    				NodeDTO childDTO = new NodeDTO(childclass.getID());
	    				childDTO.setName(childclass.getName());
	    				roots.add(childDTO);
	    			}
	    			addChildrenToNode(oboClass);
	    		}
	    		else{// a regular node
	    			addNodeToParent(oboClass);
	    			addChildrenToNode(oboClass);
	    		}
	    	}
	    }
	}
	
	/**
	 * A helper method to add a node to the list of children
	 * of a parent
	 * @param node - the node to be stored
	 */
	public void addNodeToParent(OBOClass node){
		Set<NodeDTO> children;
		//we assume a tree. 
		Link pLink = (Link)node.getParents().toArray()[0];
		OBOClass parent = (OBOClass)pLink.getParent();
		
		NodeDTO parentDTO = new NodeDTO(parent.getID());
		parentDTO.setName(parent.getName());
		
		NodeDTO nodeDTO = new NodeDTO(node.getID());
		nodeDTO.setName(node.getName());
		
		if(parentToChildrenMap.containsKey(parentDTO)){
			children = parentToChildrenMap.get(parentDTO);
		}
		else{
			children = new HashSet<NodeDTO>();
		}
		children.add(nodeDTO);
		parentToChildrenMap.put(parentDTO, children);
		childToParentMap.put(nodeDTO, parentDTO);
	}
	
	/**
	 * A helper method to add children to a node
	 * @param node - the node whose children are to be added
	 */
	public void addChildrenToNode(OBOClass node){
		Set<NodeDTO> children = new HashSet<NodeDTO>();
		
		NodeDTO nodeDTO = new NodeDTO(node.getID());
		nodeDTO.setName(node.getName());
		
		for(Link cLink : node.getChildren()){
			OBOClass child = (OBOClass)cLink.getChild();
			NodeDTO childDTO = new NodeDTO(child.getID());
			childDTO.setName(child.getName());
			children.add(childDTO);
			childToParentMap.put(childDTO, nodeDTO);
		}
		parentToChildrenMap.put(nodeDTO, children);
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
		if(!getLeaves().contains(node)){//this is not a leaf node
			for(NodeDTO child : getParentToChildrenMap().get(node)){
				bw.write(tabs + child.getId() + "\t" + child.getName() + "\n");
				printTaxonomy(child, tabCt + 1, bw);
			}
		}
		else{
			return;
		}
	}
	
	/**
	 * @PURPOSE The purpose of this method is to find all the leaf nodes under a given node in the
	 * tree
	 * @param nodeId - the id of the node we are interested in 
	 * @param tree - the tree of taxa
	 * @return the list of all the leaf nodes under the input node
	 */
	public List<NodeDTO> getLeavesUnderNode(String nodeId, TaxonTree tree){
		NodeDTO node = idToClassMapper.get(nodeId);
		List<NodeDTO> leavesUnderNode = new ArrayList<NodeDTO>();
	
		for(NodeDTO leaf : tree.getLeaves()){
			List<NodeDTO> path = this.tracePathToRoot(leaf.getId(), new ArrayList<NodeDTO>());
			if(path.contains(node))
				leavesUnderNode.add(node);
		}
		
		return leavesUnderNode;
	}
	
	/**
	 * @PURPOSE This method determines the correct
	 * path from a given TTO node (its ID to be specific eg. TTO:0001979)
	 * to the root
	 * @param ttb - an instance of the TeleostTaxonomyBuilder
	 * @param nodeId - the node to be searched for
	 * @param path - the path from the node to the root of the TTO
	 * @return the path from the given node to the root
	 */
	public List<NodeDTO> tracePathToRoot(String nodeId, List<NodeDTO> path){
		//we check if this is a root node. Exit condition for 
		//recursion 
		for(NodeDTO root : roots){
			if(root.getId().equals(nodeId)){
				path.add(root);
				return path;
			}
		}
		//otherwise, we recurse through the collection of nodes
		//and children
		NodeDTO parent = getChildToParentMap().get(idToClassMapper.get(nodeId));
		path.add(getIdToClassMapper().get(nodeId));
		return tracePathToRoot(parent.getId(), path);
	}
	
	/**
	 * @PURPOSE This method computes the path between a given node and 
	 * an arbitrary node in the tree
	 * @NOTE This is the same as {@link tracePathToRoot} except the terminus
	 * is an arbitrary node and not the root in this case
	 * @param startNodeId - the node to start from 
	 * @param endNodeId - the node to end at
	 * @param path - the path from current node to end
	 * @return the complete path from source to end
	 */
	public List<NodeDTO> tracePath(NodeDTO startNode, NodeDTO endNode, List<NodeDTO> path){
		
		if(startNode.getId().equals(endNode.getId()) ||
				!this.tracePathToRoot(startNode.getId(), new ArrayList<NodeDTO>()).contains(endNode)){
			path.add(endNode);
			return path;
		}
		
		NodeDTO parent = childToParentMap.get(startNode);
		path.add(startNode);
		return tracePath(parent, endNode, path);
	}
	
	/**
	 * @PURPOSE This method takes in a list of taxa and returns their MRCA
	 * @param taxa - the list of taxa whose MRCA is to be determined
	 * @param mrca - the current MRCA
	 * @return the MRCA of the entire list of taxa
	 */
	public NodeDTO findMRCA(LinkedList<NodeDTO> taxa, NodeDTO mrca){
		//if we have reached the end of the list,
		//or if we have already reached the root, return 
		if (mrca != null)
			System.out.println("mrca is " + mrca.getId());
		else
			System.out.println("mrca is NULL");
		
		if(taxa.size() == 0 || getRoots().contains(mrca))
			return mrca;
		//get the first element in the list
		NodeDTO first = taxa.remove(0);
		//find the mrca of the first element and the input mrca
		NodeDTO result = findMRCA(first, mrca); 
		//recursively call the same method, with the shortened list
		//and the new MRCA as arguments
		System.out.println("new MRCA is " + result.getId());
		return findMRCA(taxa, result);
	}
	
	/**
	 * @PURPOSE A helper method to find the MRCA of any two given taxa.
	 * @NOTE this is an overloaded method and is private access only
	 * @param oClass - one of the two taxa
	 * @param mrca - the other taxon
	 * @return - the mrca of the two taxa
	 */
	public NodeDTO findMRCA(NodeDTO oClass, NodeDTO mrca){
		//if the MRCA is null, we are just getting started trivially
		//return the oClass as the MRCA
		if(mrca == null)
			return oClass;
		//if the MRCA lies in the path from the oClass to the Root,
		//the MRCA stays the MRCA
		else if(this.tracePathToRoot(oClass.getId(), new ArrayList<NodeDTO>()).contains(mrca))
			return mrca;
		//otherwise, if the oClass lies in the path from the MRCA to its root,
		//the oClass becomes the new MRCA
		else if(this.tracePathToRoot(mrca.getId(), new ArrayList<NodeDTO>()).contains(oClass))
			return oClass;
		else{
			//get the path from oClass to Root
			List<NodeDTO> pathFromOclass = this.tracePathToRoot(oClass.getId(), new ArrayList<NodeDTO>());
			//get the path from mrca to Root
			List<NodeDTO> pathFromMrca = this.tracePathToRoot(mrca.getId(), new ArrayList<NodeDTO>());
			//find where they intersect, this intersection point is the new MRCA
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
	 * @PURPOSE The purpose of this method is to arrange
	 * the input list of <TAXON><ENTITY><QUALITY> triples 
	 * hierarchically in a tree with the MRCA of all the taxa 
	 * at the root. Every intermediate node is also stored in 
	 * the tree
	 * @param triplesList - input list of <TAXON><ENTITY><QUALITY>
	 * triples 
	 * @param tree - the tree of hierarchical taxa
	 * @return the completed tree given all the taxa in the input list
	 */
	public TaxonTree constructTreeFromTriplesList(List<String[]> triplesList,  
												TaxonTree tree){
		
		Map<NodeDTO, Map<NodeDTO, Set<NodeDTO>>> taxonToEQMap = 
				new HashMap<NodeDTO, Map<NodeDTO, Set<NodeDTO>>>();
		
		Map<NodeDTO, Map<NodeDTO, Set<NodeDTO>>> nodeToEQMap = 
			new HashMap<NodeDTO, Map<NodeDTO, Set<NodeDTO>>>();
		
		//get all the branching points, we'll use these
		Map<NodeDTO, Set<NodeDTO>> branches = tree.getBranchingPointsAndChildren();
		//get the count of annotations for all nodes
		Map<NodeDTO, Integer> annotationCountsMap = tree.getNodeToAnnotationCountMap(); 
		Map<NodeDTO, String[]> taxonToAnnotationMap = tree.getNodeToAnnotationMap();
		
		NodeDTO currRoot = null;
		NodeDTO taxonFromTriple, entity, quality;
		
		Set<NodeDTO> taxaSet = new HashSet<NodeDTO>();
		/*
		 * first we convert the triples list to a usable data structure, which maps
		 * every Taxon to a map of Entity to Quality tuples it is associated with. This 
		 * will be of use later
		 */
		
		for(String[] triple : triplesList){
			taxonFromTriple = idToClassMapper.get(triple[0]);
			entity = idToClassMapper.get(triple[1]);
			quality = idToClassMapper.get(triple[2]);
			
			taxaSet.add(taxonFromTriple);
			taxonToAnnotationMap.put(taxonFromTriple, new String[]{triple[1], triple[2], triple[3]});
			
			Map<NodeDTO, Set<NodeDTO>> e2qMapForTaxon;
			Set<NodeDTO> qualsForEntity;
			
			Integer annotCt = annotationCountsMap.get(taxonFromTriple);
			if(annotCt == null)
				annotCt = 0;
			
			annotationCountsMap.put(taxonFromTriple, ++annotCt);
			
			if(taxonToEQMap.containsKey(taxonFromTriple)){
				e2qMapForTaxon = taxonToEQMap.get(taxonFromTriple); 
				if(e2qMapForTaxon.containsKey(entity)){
					qualsForEntity = e2qMapForTaxon.get(entity);
				}
				else{
					qualsForEntity = new HashSet<NodeDTO>();
				}
			}
			else{
				e2qMapForTaxon = new HashMap<NodeDTO, Set<NodeDTO>>();
				qualsForEntity = new HashSet<NodeDTO>();
			}
			qualsForEntity.add(quality);
			e2qMapForTaxon.put(entity, qualsForEntity);
			taxonToEQMap.put(taxonFromTriple, e2qMapForTaxon);
		}
		
		/*
		 * Now we start working with this data structure
		 */
		for(NodeDTO taxon : taxaSet){	
			Map<NodeDTO, Map<NodeDTO, Set<NodeDTO>>> nodeToEQMap1 = 
				new HashMap<NodeDTO, Map<NodeDTO, Set<NodeDTO>>>(), nodeToEQMap2 = null;
			
			NodeDTO oldRoot = currRoot; 
			//find the MRCA
			currRoot = this.findMRCA(taxon, currRoot);
			//find path to current MRCA from this taxon
			List<NodeDTO> pathToMrca = this.tracePath(taxon, currRoot, new ArrayList<NodeDTO>());
			//process each node in the path
			branches = processNodesInPathToAddBranches(pathToMrca, branches);
			nodeToEQMap1 = processNodesInPathForEQ(pathToMrca, taxonToEQMap);
			//if old root is not null, also find path to current MRCA from old MRCA
			if(oldRoot != null){
				List<NodeDTO> oldPathToMrca = this.tracePath(oldRoot, currRoot, new ArrayList<NodeDTO>());
				//process each node in this path
				branches = processNodesInPathToAddBranches(oldPathToMrca, branches);
				nodeToEQMap2 = processNodesInPathForEQ(oldPathToMrca, taxonToEQMap);
			}
			
			nodeToEQMap.putAll(nodeToEQMap1);
			if(nodeToEQMap2 != null)
				nodeToEQMap.putAll(nodeToEQMap2);
		}
		//set the branches of the tree and the root
		tree.setBranchingPointsAndChildren(branches);
		tree.setRoot(currRoot);
		//set the EQ details for every node in the tree
		tree.setNodeToEQMap(nodeToEQMap);
		
//		tree.getNodeToAnnotationCountMap().put(currRoot, triplesList.size());
		
		//here we update the annotation counts for every node of the tree except the leaf nodes
		for(NodeDTO leaf : taxaSet){
			Integer ct = 0;
			//get the number of annotations for this leaf
			Map<NodeDTO, Set<NodeDTO>> e2qMap4Leaf = tree.getNodeToEQMap().get(leaf);
			for(NodeDTO e : e2qMap4Leaf.keySet()){
				ct += e2qMap4Leaf.get(e).size();
			}
			//get the path from the leaf to the root
			List<NodeDTO> pathToRoot = this.tracePath(leaf, currRoot, new ArrayList<NodeDTO>());
			//update counts for each node in this path
			for(NodeDTO node : pathToRoot){
				//except the leaf
				if(!node.equals(leaf)){
					Integer ct4Node = tree.getNodeToAnnotationCountMap().get(node);
					if(ct4Node == null)
						ct4Node = 0;
					ct4Node += ct;
					tree.getNodeToAnnotationCountMap().put(node, ct4Node);
				}
			}
		}
		return tree;
	}
	
	/**
	 * @PURPOSE This is another helper method, which takes in a set of mappings
	 * from leaf OBOClasses to EQ combinations. It consolidates these
	 * into a set of mappings for the entire tree, given the paths
	 * from each leaf node to the root of the tree as a second 
	 * argument
	 * @param path - the path comprising OBOClasses from the leaf nodes to 
	 * the root of the tree
	 * @param eqMap - an input set of mappings from leaf taxa to EQ combinations
	 * @return a consolidated mapping from every taxa in the tree to EQ combinations
	 */
	
	private Map<NodeDTO, Map<NodeDTO, Set<NodeDTO>>> processNodesInPathForEQ(
			List<NodeDTO> path,
			Map<NodeDTO, Map<NodeDTO, Set<NodeDTO>>> eqMap) {

		Map<NodeDTO, Map<NodeDTO, Set<NodeDTO>>> node2EQMap = eqMap;
		
		Map<NodeDTO, Set<NodeDTO>> e2qMap4Node, e2qMap4Parent;
		Set<NodeDTO> qSet4E, qSet4P = null;
		
		for(int index = 0; index < path.size() - 1; index++){
			NodeDTO node = path.get(index);
			NodeDTO parent = path.get(index + 1);
			
			e2qMap4Node = node2EQMap.get(node);
			e2qMap4Parent = node2EQMap.get(parent);
			
			if(e2qMap4Node != null){
				for(NodeDTO e : e2qMap4Node.keySet()){
					qSet4E = e2qMap4Node.get(e);
					if(e2qMap4Parent != null){
						qSet4P = e2qMap4Parent.get(e);
					}
					else{
						e2qMap4Parent = new HashMap<NodeDTO, Set<NodeDTO>>();
					}
					if(qSet4P == null)
						qSet4P = new HashSet<NodeDTO>();
					qSet4P.addAll(qSet4E);
					e2qMap4Parent.put(e, qSet4P);
				}
			}
			
			node2EQMap.put(node, e2qMap4Node);
			node2EQMap.put(parent, e2qMap4Parent);
		}
		return node2EQMap;
	}

	/**
	 * @PURPOSE This is a helper method. It takes in a set of paths from 
	 * leaf nodes of a tree to the root, and outputs the entire tree, 
	 * including information about branching nodes and children  
	 * @param path - the list of paths from all leaf nodes to the root of the tree
	 * @param branches - the map of branching points and their children
	 * @return - the updated data about branches
	 */
	private Map<NodeDTO, Set<NodeDTO>> processNodesInPathToAddBranches(
			List<NodeDTO> path, Map<NodeDTO, Set<NodeDTO>> branches) {
		Set<NodeDTO> children;
		for(int index = 0; index < path.size() - 1; index++){
			NodeDTO node = path.get(index);
			//get the parent of that node
			NodeDTO parent = path.get(index + 1);
			//get the annotationcounts for the node
			
			if(branches.containsKey(parent))
				children = branches.get(parent);
			else
				children = new HashSet<NodeDTO>();
			children.add(node);
			branches.put(parent, children);
		}
		return branches;
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

		long startTime1 = System.currentTimeMillis();
		TeleostTaxonomyBuilder ttb = new TeleostTaxonomyBuilder();
		long endTime1 = System.currentTimeMillis();
		
		ttb.log().trace((endTime1 - startTime1) + " milliseconds to set up taxonomy");
		
//		System.out.println(ttb.tracePath("TTO:1001979", new ArrayList<OBOClass>()));
//		System.out.println(ttb.tracePath("TTO:1002351", new ArrayList<OBOClass>()));
//		System.out.println(ttb.tracePath("TTO:10000039", new ArrayList<OBOClass>()));

		
		LinkedList<NodeDTO> ttoList = new LinkedList<NodeDTO>();
		Iterator<String> it = ttb.getIdToClassMapper().keySet().iterator();
		
		List<String[]> triplesList = new ArrayList<String[]>();
		triplesList.add(new String[]{"TTO:1001979", "TAO:0001173", "PATO:0000462", "123456"});
		triplesList.add(new String[]{"TTO:1002351", "TAO:0001510", "PATO:0000467", "234567"});
		triplesList.add(new String[]{"TTO:1005577", "TAO:0001188", "PATO:0001452", "345678"});
		triplesList.add(new String[]{"TTO:1005381", "TAO:0000514", "PATO:0000052", "456789"});
		triplesList.add(new String[]{"TTO:1067707", "TAO:0001510", "PATO:0000462", "567890"});
		triplesList.add(new String[]{"TTO:1063327", "TAO:0000250", "PATO:0000587", "111111"});
		triplesList.add(new String[]{"TTO:1021209", "TAO:0001510", "PATO:0000462", "222222"});
		triplesList.add(new String[]{"TTO:1006313", "TAO:0001510", "PATO:0000467", "333333"});
		
		while(it.hasNext()){
			String tto = it.next();
			ttoList.add(ttb.getIdToClassMapper().get(tto));
		}

//		ttoList.add(ttb.getIdToClassMapper().get("TTO:1001979"));
//		ttoList.add(ttb.getIdToClassMapper().get("TTO:1002351"));
//		ttoList.add(ttb.getIdToClassMapper().get("TTO:10000039"));
//		long startTime2 = System.currentTimeMillis();
		
		
		System.out.println(ttb.findMRCA(ttoList, null));
//		long endTime2 = System.currentTimeMillis();
//		System.out.println((endTime2 - startTime2) + " milliseconds to find MRCA");
		
		TaxonTree tree = new TaxonTree();
		
		long startTime2 = System.currentTimeMillis();
		tree = ttb.constructTreeFromTriplesList(triplesList, tree);
		long endTime2 = System.currentTimeMillis();
		System.out.println((endTime2 - startTime2) + " milliseconds to construct subtree from taxonList");
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File("/home/cartik/Desktop/subtree.txt")));
		System.out.println("root of the tree is " + tree.getRoot());
		System.out.println("number of annotations for root " + tree.getRoot() + " is " + tree.getNodeToAnnotationCountMap().get(tree.getRoot()));
		System.out.println("annotations for root " + tree.getRoot() + " are " + tree.getNodeToEQMap().get(tree.getRoot()));
		tree.printTaxonomy(tree.getRoot(), 0, bw);
		bw.flush();
		bw.close();
	}

	private Logger log() {
		return Logger.getLogger(this.getClass());
	}
}
