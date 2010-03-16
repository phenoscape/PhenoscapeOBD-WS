package org.obd.ws.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bbop.dataadapter.DataAdapterException;
import org.obd.ws.util.dto.NodeDTO;
import org.obo.dataadapter.OBOAdapter;
import org.obo.dataadapter.OBOFileAdapter;
import org.obo.datamodel.Link;
import org.obo.datamodel.OBOClass;
import org.obo.datamodel.OBOSession;
import org.obo.datamodel.PropertyValue;
import org.obo.util.TermUtil;

public class TTOTaxonomy {
	/** This variable is available only within this class */
	private OBOSession oboSession;
	
	private NodeDTO root;
	private Set<NodeDTO> leaves;
	/** This structure keeps track of parent nodes and their children */
	private Map<NodeDTO, Set<NodeDTO>> nodeToChildrenMap;
	/** This structure is a reverse lookup mapping children nodes to their resp parents */
	private Map<NodeDTO, NodeDTO> nodeToParentMap;
	/** This is a lookup table from a String id to the Node */
	private Map<String, NodeDTO> idToNodeMap;
	/** This is the set of EVERY node in the taxonomy */
	private Set<NodeDTO> nodes;
	/** This structure keeps track of the path from each node to the root of the taxonomy*/
	private Map<NodeDTO, List<NodeDTO>> nodeToPathMap;
	/** This structure maps every taxon in the TTO to its proper rank */
	private Map<NodeDTO, NodeDTO> taxonToRankMap;
	/** This structure keeps track of extinct taxa */
	private Set<NodeDTO> setOfExtinctTaxa;
	
	private static final String TTO_URL_STRING = 
			"http://obo.cvs.sourceforge.net/*checkout*/obo/obo/ontology/taxonomy/teleost_taxonomy.obo";
	private static final String TAXONOMIC_RANK_URL_STRING = 
			"http://phenoscape.svn.sourceforge.net/viewvc/phenoscape/trunk/vocab/taxonomic_rank.obo";
	
	private static final String TTO_FILE_NAME = "/tmp/teleost_taxonomy.obo";
	private static final String TAXONOMIC_RANK_FILE_NAME = "/tmp/taxonomic_rank.obo";
	
	private static final String RANK_NAMESPACE = "taxonomic_rank";
	
	private static final String HAS_RANK_RELATION_STRING = "has_rank";
	private static final String IS_EXTINCT_RELATION_STRING = "is_extinct";
	/* GETTERS and SETTERs */
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

	public Map<NodeDTO, Set<NodeDTO>> getNodeToChildrenMap() {
		return nodeToChildrenMap;
	}

	public void setNodeToChildrenMap(
			Map<NodeDTO, Set<NodeDTO>> nodeToChildrenMap) {
		this.nodeToChildrenMap = nodeToChildrenMap;
	}

	public Map<NodeDTO, NodeDTO> getNodeToParentMap() {
		return nodeToParentMap;
	}

	public void setChildToParentMap(Map<NodeDTO, NodeDTO> nodeToParentMap) {
		this.nodeToParentMap = nodeToParentMap;
	}
	
	public Map<NodeDTO, List<NodeDTO>> getNodeToPathMap() {
		return nodeToPathMap;
	}
	public void setNodeToPathMap(Map<NodeDTO, List<NodeDTO>> nodeToPathMap) {
		this.nodeToPathMap = nodeToPathMap;
	}

	public Map<String, NodeDTO> getIdToNodeMap() {
		return idToNodeMap;
	}
	
	public Map<NodeDTO, NodeDTO> getTaxonToRankMap() {
		return taxonToRankMap;
	}

	public Set<NodeDTO> getSetOfExtinctTaxa() {
		return setOfExtinctTaxa;
	}
	
	/**
	 * Constructor initializes instance variables and calls the methods to
	 * update all of them with information from the ontology
	 * @throws IOException 
	 * @throws DataAdapterException 
	 */
	public TTOTaxonomy() throws IOException, DataAdapterException{
		nodes = new HashSet<NodeDTO>();
		leaves = new HashSet<NodeDTO>();
		nodeToChildrenMap = new HashMap<NodeDTO, Set<NodeDTO>>();
		nodeToParentMap = new HashMap<NodeDTO, NodeDTO>();
		idToNodeMap = new HashMap<String, NodeDTO>();
		nodeToPathMap = new HashMap<NodeDTO, List<NodeDTO>>();
		taxonToRankMap = new HashMap<NodeDTO, NodeDTO>();
		setOfExtinctTaxa = new HashSet<NodeDTO>();
		
		readOntologyIntoLocalFile();
		createSessionFromOntology();
		populateRankObjectsFromSession();
		createTaxonomyFromSession();
		tracePathsToRootForAllNodes();
	}
	
	/**
	 * This method reads the ontology from a remote URL 
	 * and transcribes it to a local file
	 * @throws IOException
	 */
	private void readOntologyIntoLocalFile() throws IOException{
		readOntologyIntoLocalFile(TTO_URL_STRING, TTO_FILE_NAME);
		readOntologyIntoLocalFile(TAXONOMIC_RANK_URL_STRING, TAXONOMIC_RANK_FILE_NAME);
	}

	private void readOntologyIntoLocalFile(String urlString, String fileLocationString) throws IOException{
		URL url = new URL(urlString);
		BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));
		File file = new File(fileLocationString);
		

		String lineFromWebPage;
		
		BufferedWriter writer = new BufferedWriter(new FileWriter(file));
		
		while((lineFromWebPage = reader.readLine()) != null){
			writer.write(lineFromWebPage + "\n");
		}
		
		writer.flush();
		writer.close();
	}
	/**
	 * This method reads the local ontology file and 
	 * creates an OBOSession instance to store this ontology
	 * @throws DataAdapterException
	 */
	private void createSessionFromOntology() throws DataAdapterException{
		List<String> paths = new ArrayList<String>();
		paths.add(TTO_FILE_NAME);
		paths.add(TAXONOMIC_RANK_FILE_NAME);
		
		//setting up the file adapter and its configuration
		final OBOFileAdapter fileAdapter = new OBOFileAdapter();
		OBOFileAdapter.OBOAdapterConfiguration config = new OBOFileAdapter.OBOAdapterConfiguration();
	    config.setReadPaths(paths);
	    config.setBasicSave(false);
	    config.setAllowDangling(true);
	    config.setFollowImports(false);
	    
	    oboSession = fileAdapter.doOperation(OBOAdapter.READ_ONTOLOGY, config, null);
	}
	
	/**
	 * This method takes in every oboClass from the session and arranges them 
	 * into a taxonomy
	 */
	private void createTaxonomyFromSession(){
		for(OBOClass oboClass : TermUtil.getTerms(oboSession)){
	    	if(oboClass.getNamespace() != null && !oboClass.isObsolete() &&
	    			oboClass.getID().matches("TTO:[0-9]+")){
	    		
	    		NodeDTO nodeDTO = createNodeDTOFromOBOClass(oboClass);
	    		nodes.add(nodeDTO);
	    		idToNodeMap.put(oboClass.getID(), nodeDTO);
	    		
	    		extractRankForTaxon(oboClass);
	    		checkIfTaxonIsExtinct(oboClass);
	    		
	    		if(oboClass.getChildren().size() == 0){//a leaf node
	    			leaves.add(nodeDTO);
	    			addNodeToParent(oboClass);
	    		}
	    		else if(oboClass.getParents().size() == 0){//a root node
	    			setRoot(nodeDTO);
	    			addChildrenToNode(oboClass);
	    		}
	    		else{
	    			addNodeToParent(oboClass);
	    			addChildrenToNode(oboClass);
	    		}
	    	}
		}
	}
	
	private void populateRankObjectsFromSession(){
		for(OBOClass oboClass : TermUtil.getTerms(oboSession)){
	    	if(oboClass.getNamespace() != null && !oboClass.isObsolete() &&
	    			(oboClass.getNamespace().toString().trim().equals(RANK_NAMESPACE))){
	    		NodeDTO rankDTO = createNodeDTOFromOBOClass(oboClass);
	    		idToNodeMap.put(oboClass.getID(), rankDTO);
	    	}
		}
	}
	
	/**
	 * These two methods extract the rank information for a taxon and adds it to a 
	 * map 
	 * @param oboClass - the input taxon
	 */
	private void extractRankForTaxon(OBOClass oboClass){
		for(PropertyValue propertyValue : oboClass.getPropertyValues()){
			if(propertyValue.getValue().contains(HAS_RANK_RELATION_STRING)){
				String propValue = propertyValue.getValue();
				 if(propValue != null && propValue.trim() != ""){
					 assignRankToTaxon(propValue, oboClass);
				 }
			}
		}
	}
	
	private void assignRankToTaxon(String propValue, OBOClass oboClass) {
		String[] propValueComponents = propValue.split("\\s");
		if(propValueComponents.length == 2){
			NodeDTO taxonDTO = new NodeDTO(oboClass.getID());
			taxonDTO.setName(oboClass.getName());
			NodeDTO rankDTO = new NodeDTO(propValueComponents[1]);
			String rankLabel = idToNodeMap.get(propValueComponents[1]).getName();
			rankDTO.setName(rankLabel);
			taxonToRankMap.put(taxonDTO, rankDTO);
		}
	}

	/**
	 * This method checks if the input taxon is extinct. If it is, this is 
	 * added to a set of extinct taxa
	 * @param oboClass - the input taxon
	 */
	private void checkIfTaxonIsExtinct(OBOClass oboClass){
		for(PropertyValue propertyValue : oboClass.getPropertyValues()){
			if(propertyValue.getValue().contains(IS_EXTINCT_RELATION_STRING)){
				NodeDTO extinctTaxon = new NodeDTO(oboClass.getID());
				extinctTaxon.setName(oboClass.getName());
				setOfExtinctTaxa.add(extinctTaxon);
			}
		}
	}
	
	/**
	 * This method traces paths to the root of the taxonomy
	 * for every node in the taxonomy and updates 
	 * the data structure, which holds this information
	 */
	private void tracePathsToRootForAllNodes(){
		for(NodeDTO node : nodes){
			List<NodeDTO> pathToRoot = 
				tracePathToRootForNode(node, new ArrayList<NodeDTO>());
			nodeToPathMap.put(node, pathToRoot);
		}
	}
	
	/**
	 * This method traces a path from the input node to the root
	 * of the taxonomy
	 * @param node
	 * @param path
	 * @return a path from the given node to the root of the tree
	 */
	private List<NodeDTO> tracePathToRootForNode(NodeDTO node, List<NodeDTO> path){
		path.add(node);
		if(nodeToParentMap.get(node) == null)
			return path;
		NodeDTO parent = nodeToParentMap.get(node);
		return tracePathToRootForNode(parent, path);
	}
	
	/**
	 * This method finds the parent of the input OBOClass
	 * and updates the local data structures
	 * @param oboClass 
	 */
	private void addNodeToParent(OBOClass oboClass){
		NodeDTO nodeDTO = createNodeDTOFromOBOClass(oboClass);
		Link pLink = (Link)oboClass.getParents().toArray()[0];
		OBOClass parentOBOClass = (OBOClass)pLink.getParent();
		NodeDTO parentDTO = createNodeDTOFromOBOClass(parentOBOClass);
		nodeToParentMap.put(nodeDTO, parentDTO);
	}
	
	/**
	 * This method finds the children of the input
	 * OBOClass and updtes the local data structures 
	 * @param oboClass
	 */
	private void addChildrenToNode(OBOClass oboClass){
		Set<NodeDTO> children;
		NodeDTO nodeDTO = createNodeDTOFromOBOClass(oboClass);
		if(nodeToChildrenMap.containsKey(nodeDTO))
			children = nodeToChildrenMap.get(nodeDTO);
		else
			children = new HashSet<NodeDTO>();
		for(Link cLink : oboClass.getChildren()){
			OBOClass child = (OBOClass)cLink.getChild();
			NodeDTO childDTO = createNodeDTOFromOBOClass(child);
			children.add(childDTO);
		}
		nodeToChildrenMap.put(nodeDTO, children);
	}

	/**
	 * A utility method to create a NodeDTO object from the 
	 * input OBOClass
	 * @param oboClass 
	 * @return DTO object for the given oboClass
	 */
	private NodeDTO createNodeDTOFromOBOClass(OBOClass oboClass){
		NodeDTO nodeDTO = new NodeDTO(oboClass.getID());
		nodeDTO.setName(oboClass.getName());
		return nodeDTO;
	}
	
	/**
	 * The main method has been written to test if all the
	 * structures in this class have been properly updates 
	 * and ready for use
	 * @param args
	 */
	public static void main(String[] args){
		try{
			TTOTaxonomy ttoTree = new TTOTaxonomy();
			NodeDTO node = ttoTree.getIdToNodeMap().get("TTO:11021");
			List<NodeDTO> pathToRoot = ttoTree.getNodeToPathMap().get(node);
			System.out.println("Root is " + ttoTree.getRoot());
			System.out.println(node); 
			System.out.println(pathToRoot);
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
