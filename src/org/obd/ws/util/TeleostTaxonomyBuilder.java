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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bbop.dataadapter.DataAdapterException;
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
	
	private Map<OBOClass, Set<OBOClass>> nodesWithChildren;

	private Set<OBOClass> roots;
	private Set<OBOClass> leaves;

	/*
	 * GETTER methods for instance variables
	 */
	public Set<OBOClass> getRoots() {
		return roots;
	}

	public Set<OBOClass> getLeaves() {
		return leaves;
	}
	
	public Map<OBOClass, Set<OBOClass>> getNodesWithChildren() {
		return nodesWithChildren;
	}
	
	/**
	 * The default constructor reads in the ontology from the 
	 * URL and stores it in a file. It also intializes the
	 * instance variables
	 * @throws IOException
	 */
	public TeleostTaxonomyBuilder() throws IOException{
		nodesWithChildren = new HashMap<OBOClass, Set<OBOClass>>();
		roots = new HashSet<OBOClass>();
		leaves = new HashSet<OBOClass>();
		
		URL ttoURL = new URL("http://www.berkeleybop.org/ontologies/obo-all/teleost_taxonomy/teleost_taxonomy.obo");
		BufferedReader br = new BufferedReader(new InputStreamReader(ttoURL.openStream()));
		
		String line; 
		File ttoFile = new File("teleost_taxonomy.obo");
		
		BufferedWriter pw = new BufferedWriter(new FileWriter(ttoFile));
		
		while((line = br.readLine()) != null){
			pw.write(line + "\n");
		}
		
		pw.flush();
		pw.close();
	}
	
	/**
	 * This method reads in the TTO and arranges the terms in 
	 * a taxonomy
	 * @throws DataAdapterException 
	 * @throws IOException 
	 */
	public void setUpTaxonomy() throws DataAdapterException, IOException{
		
		List<String> paths = new ArrayList<String>();
		paths.add("teleost_taxonomy.obo");
		
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
	    			oboClass.getID().matches("TTO:[0-9]+")){
	    		if(oboClass.getChildren().size() == 0){//a leaf node
	    			leaves.add(oboClass);
	    			addNodeToParent(oboClass);
	    		}
	    		else if(oboClass.getParents().size() == 0){//a root node
	    			roots.add(oboClass);
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
		Set<OBOClass> children;
		//we assume a tree. 
		Link pLink = (Link)node.getParents().toArray()[0];
		OBOClass parent = (OBOClass)pLink.getParent();
		if(nodesWithChildren.containsKey(parent)){
			children = nodesWithChildren.get(parent);
		}
		else{
			children = new HashSet<OBOClass>();
		}
		children.add(node);
		nodesWithChildren.put(parent, children);
	}
	
	/**
	 * A helper method to add children to a node
	 * @param node - the node whose children are to be added
	 */
	public void addChildrenToNode(OBOClass node){
		Set<OBOClass> children = new HashSet<OBOClass>();
		
		for(Link cLink : node.getChildren()){
			OBOClass child = (OBOClass)cLink.getChild();
			children.add(child);
		}
		
		nodesWithChildren.put(node, children);
	}
	
	/**
	 * @PURPOSE This is a test method to print out the entire
	 * stored taxonomy with the proper indentation
	 * @CAUTION Hardcoded file path for writing out the taxonomy
	 * @param ttb - this instance of the TeleostTaxonomyBuilder class
	 * @param node - the node whose children are to printed along with the node
	 * itself
	 * @param tabCt - the number of indents
	 * @param bw - buffered reader which contains a pointer to a text file
	 * @throws IOException
	 */
	public void printTaxonomy(TeleostTaxonomyBuilder ttb, OBOClass node, int tabCt, BufferedWriter bw) 
		throws IOException{
		String tabs = "";

		for(int i = 0; i < tabCt; i++){
			tabs += "  ";
		}
		if(!ttb.getLeaves().contains(node)){//this is not a leaf node
			for(OBOClass child : ttb.getNodesWithChildren().get(node)){
				bw.write(tabs + child.getID() + "\t" + child.getName() + "\n");
				printTaxonomy(ttb, child, tabCt + 1, bw);
			}
		}
		else{
			return;
		}
	}
	
	/**
	 * @PURPOSE This method is a test method to verify the correct
	 * path from a given TTO node (its ID to be specific eg. TTO:0001979)
	 * to the root
	 * @param ttb - an instance of the TeleostTaxonomyBuilder
	 * @param nodeId - the node to be searched for
	 * @param path - the path from the node to the root of the TTO
	 */
	public void tracePath(TeleostTaxonomyBuilder ttb, String nodeId, String path){
		
		//we check if this is a root node. Exit condition for 
		//recursion
		Iterator<OBOClass> rootIt = ttb.getRoots().iterator();
		while(rootIt.hasNext()){
			OBOClass root = rootIt.next();
			if(root.getID().equals(nodeId)){
				System.out.println(path);
				return;
			}
		}

		//otherwise, we recurse through the collection of nodes
		//and children
		for(OBOClass key : ttb.getNodesWithChildren().keySet()){
			Set<OBOClass> children = ttb.getNodesWithChildren().get(key);
			for(OBOClass child : children){
				if(child.getID().equals(nodeId)){
					path += nodeId + "[" + child.getName() + "]" + "\n";
					tracePath(ttb, key.getID(), path);
				}
			}
		}
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
		File printFile = new File("/home/cartik/Desktop/TTO_hierarchy.txt");
		BufferedWriter bw = new BufferedWriter(new FileWriter(printFile));

		TeleostTaxonomyBuilder ttb = new TeleostTaxonomyBuilder();
		ttb.setUpTaxonomy();
		for(OBOClass root : ttb.getRoots()){
			ttb.printTaxonomy(ttb, root, 0, bw);
		}
		bw.flush();
		bw.close();
		
		ttb.tracePath(ttb, "TTO:1001979", "");
	}
}
