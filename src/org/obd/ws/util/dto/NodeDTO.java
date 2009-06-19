package org.obd.ws.util.dto;

public class NodeDTO {
	
	private String id;
	private String name;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	//ID is read only
	public String getId() {
		return id;
	}
	
	public NodeDTO(String id){
		this.id = id; 
	}
}
