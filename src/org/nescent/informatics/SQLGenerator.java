package org.nescent.informatics;

import org.obd.model.LinkStatement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * FIXME Class comment missing.
 */
public class SQLGenerator {
	
	private List<LinkStatement> linkStatementList;
	private String sqlStr;
	
        /**
         * FIXME Constructor and parameter documentation missing.
         */
	public SQLGenerator(){
		linkStatementList = new ArrayList<LinkStatement>();
		sqlStr = null; // not needed - it's Java ...
	}
	
        /**
         * FIXME Constructor and parameter documentation missing.
         */
	public SQLGenerator(List<LinkStatement> lss){
            // FIXME doesn't chain the constructor 

            // FIXME any reason why aren't calling our own setter
            // here, given that there is one?

		linkStatementList = lss;
	}
	
        /**
         * FIXME Constructor and parameter documentation missing.
         */
	public SQLGenerator(LinkStatement ls){
		this();
                // FIXME should be using our own getter - otherwise we
                // are bypassing derived classes that override it. If
                // you don't want that, make the getter final.
		linkStatementList.add(ls);
	}
	
        /**
         * FIXME Method and parameter documentation missing.
         */
	public List<LinkStatement> getLinkStatementList() {
		return linkStatementList;
	}
	
        /**
         * FIXME Method and parameter documentation missing.
         */
	public void setLinkStatementList(List<LinkStatement> linkStatementList) {
		this.linkStatementList = linkStatementList;
	}
	
        /**
         * FIXME Method and parameter documentation missing.
         */
	public String getSqlStr() {
		return sqlStr;
	}

        /**
         * FIXME Method and parameter documentation missing.
         */
	public void setSqlStr(String sqlStr) {
		this.sqlStr = sqlStr;
	}
	
        /**
         * FIXME Method and parameter documentation missing.
         */
	public List<String> constructPaths(List<String> paths, List<LinkStatement> lsList){

            // FIXME what if lsList is null?
		if(lsList.size() == 0){
			return paths;
		}
			
		LinkStatement ls = lsList.remove(0);

		String nodeId = ls.getNodeId();
		String relId = ls.getRelationId();
		String targetId = ls.getTargetId();

		if(paths == null){
			paths = new ArrayList<String>();
			paths.add(nodeId + "->" + relId + "->" + targetId);
			return constructPaths(paths, lsList);
		}
		
		for(String path : paths){
			List<String> nodesAndLinks = Arrays.asList(path.split("->"));
			if(nodesAndLinks.contains(nodeId) && nodesAndLinks.contains(targetId) && nodesAndLinks.contains(relId)){
				continue;
			}
			else if(nodesAndLinks.contains(nodeId)){
				if(nodesAndLinks.indexOf(nodeId) == (nodesAndLinks.size() - 1)){
					String oldPath = path;
					path = oldPath + "->" + relId + "->" +targetId;
					paths.remove(oldPath);
					paths.add(path);

				}
				else
					paths.add(nodeId + "->" + relId + "->" + targetId);
			}
			else if(nodesAndLinks.contains(targetId)){
				if(nodesAndLinks.indexOf(targetId) == 0){
					String oldPath = path;
					paths.remove(oldPath);
					path = nodeId + "->" + relId + "->" + oldPath;
					paths.add(path);
					
				}
				else
					paths.add(nodeId + "->" + relId + "->" + targetId);
			}
			else{
					paths.add(nodeId + "->" + relId + "->" + targetId);
			}
			return constructPaths(paths, lsList);
		}
		
		return null;
	}
	
        /**
         * FIXME Method and parameter documentation missing.
         */
	public String constructSQLQuery(){
		String sql = "";
		for(LinkStatement ls : linkStatementList){
			 sql += ls.getNodeId() + "\t";
		}
		return sql;
	}

}
