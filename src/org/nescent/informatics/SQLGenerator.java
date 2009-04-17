package org.nescent.informatics;

import org.obd.model.LinkStatement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SQLGenerator {
	
	private List<LinkStatement> linkStatementList;
	private String sqlStr;
	
	public SQLGenerator(){
		linkStatementList = new ArrayList<LinkStatement>();
		sqlStr = null;
	}
	
	public SQLGenerator(List<LinkStatement> lss){
		linkStatementList = lss;
	}
	
	public SQLGenerator(LinkStatement ls){
		this();
		linkStatementList.add(ls);
	}
	
	public List<LinkStatement> getLinkStatementList() {
		return linkStatementList;
	}
	
	public void setLinkStatementList(List<LinkStatement> linkStatementList) {
		this.linkStatementList = linkStatementList;
	}
	
	public String getSqlStr() {
		return sqlStr;
	}

	public void setSqlStr(String sqlStr) {
		this.sqlStr = sqlStr;
	}
	
	public List<String> constructPaths(List<String> paths, List<LinkStatement> lsList){
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
	
	public String constructSQLQuery(){
		String sql = "";
		for(LinkStatement ls : linkStatementList){
			 sql += ls.getNodeId() + "\t";
		}
		return sql;
	}

}
