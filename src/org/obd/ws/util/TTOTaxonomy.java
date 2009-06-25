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
import org.obo.util.TermUtil;

public class TTOTaxonomy {
	/** This variable is available only within this class */
	private OBOSession oboSession;
	
	private NodeDTO root;
	private Set<NodeDTO> leaves;
	/** This structure keeps track of parent nodes and their children */
	private Map<NodeDTO, Set<NodeDTO>> parentToChildrenMap;
	/** This structure is a reverse lookup mapping children nodes to their resp parents */
	private Map<NodeDTO, NodeDTO> childToParentMap;
	/** This is a lookup table from a String id to the Node */
	private Map<String, NodeDTO> idToNodeMap;
	/** This is the set of EVERY node in the taxonomy */
	private Set<NodeDTO> nodes;
	
	private static final String TTO_URL_STRING = 
			"http://obo.cvs.sourceforge.net/*checkout*/obo/obo/ontology/taxonomy/teleost_taxonomy.obo";
	private static final String TTO_FILE_NAME = "teleost_taxonomy.obo";

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

	public Map<NodeDTO, Set<NodeDTO>> getParentToChildrenMap() {
		return parentToChildrenMap;
	}

	public void setParentToChildrenMap(
			Map<NodeDTO, Set<NodeDTO>> parentToChildrenMap) {
		this.parentToChildrenMap = parentToChildrenMap;
	}

	public Map<NodeDTO, NodeDTO> getChildToParentMap() {
		return childToParentMap;
	}

	public void setChildToParentMap(Map<NodeDTO, NodeDTO> childToParentMap) {
		this.childToParentMap = childToParentMap;
	}
	
	/**
	 * Constructor initializes instance variables
	 */
	public TTOTaxonomy(){
		nodes = new HashSet<NodeDTO>();
		leaves = new HashSet<NodeDTO>();
		parentToChildrenMap = new HashMap<NodeDTO, Set<NodeDTO>>();
		childToParentMap = new HashMap<NodeDTO, NodeDTO>();
		idToNodeMap = new HashMap<String, NodeDTO>();
	}
	
	/**
	 * This method reads the ontology from a remote URL 
	 * and transcribes it to a local file
	 * @throws IOException
	 */
	public void readOntologyIntoLocalFile() throws IOException{
		URL ttoURL = new URL(TTO_URL_STRING);
		BufferedReader reader = new BufferedReader(new InputStreamReader(ttoURL.openStream()));
		File ttoFile = new File(TTO_FILE_NAME);
		
		String lineFromWebPage;
		
		BufferedWriter writer = new BufferedWriter(new FileWriter(ttoFile));
		
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
	public void readOntologyFile() throws DataAdapterException{
		List<String> paths = new ArrayList<String>();
		paths.add(TTO_FILE_NAME);
		
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
	public void createTaxonomyFromSession(){
		for(OBOClass oboClass : TermUtil.getTerms(oboSession)){
	    	if(oboClass.getNamespace() != null && !oboClass.isObsolete() &&
	    			oboClass.getID().matches("TTO:[0-9]+")){
	    		
	    		NodeDTO nodeDTO = createNodeDTOFromOBOClass(oboClass);
	    		nodes.add(nodeDTO);
	    		idToNodeMap.put(oboClass.getID(), nodeDTO);
	    		
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
	
	/**
	 * This method finds the parent of the input OBOClass
	 * and updates the local data structures
	 * @param oboClass 
	 */
	public void addNodeToParent(OBOClass oboClass){
		Set<NodeDTO> children;
		NodeDTO nodeDTO = createNodeDTOFromOBOClass(oboClass);
		Link pLink = (Link)oboClass.getParents().toArray()[0];
		OBOClass parentOBOClass = (OBOClass)pLink.getParent();
		NodeDTO parentDTO = createNodeDTOFromOBOClass(parentOBOClass);
		if(parentToChildrenMap.containsKey(parentDTO))
			children = parentToChildrenMap.get(parentDTO);
		else
			children = new HashSet<NodeDTO>();
		children.add(nodeDTO);
		parentToChildrenMap.put(parentDTO, children);
		childToParentMap.put(nodeDTO, parentDTO);
	}
	
	/**
	 * This method finds the children of the input
	 * OBOClass and updtes the local data structures 
	 * @param oboClass
	 */
	public void addChildrenToNode(OBOClass oboClass){
		Set<NodeDTO> children = new HashSet<NodeDTO>();
		NodeDTO nodeDTO = createNodeDTOFromOBOClass(oboClass);
		for(Link cLink : oboClass.getChildren()){
			OBOClass child = (OBOClass)cLink.getChild();
			NodeDTO childDTO = createNodeDTOFromOBOClass(child);
			children.add(childDTO);
			childToParentMap.put(childDTO, nodeDTO);
		}
		parentToChildrenMap.put(nodeDTO, children);
	}

	/**
	 * A utility method to create a NodeDTO object from the 
	 * input OBOClass
	 * @param oboClass
	 * @return
	 */
	private NodeDTO createNodeDTOFromOBOClass(OBOClass oboClass){
		NodeDTO nodeDTO = new NodeDTO(oboClass.getID());
		nodeDTO.setName(oboClass.getName());
		return nodeDTO;
	}
}
