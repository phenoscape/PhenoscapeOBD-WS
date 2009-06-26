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
	
	@Override
	public String toString(){
		return (this.id + " [" + this.name + "]");
	}
	
	@Override
	public boolean equals(Object node){
		if(this == node) return true;
		if(!(node instanceof NodeDTO)) return false;
		NodeDTO ndto = (NodeDTO)node;
		if(ndto.getId().equals(this.getId()) &&
				ndto.getName().equals(this.getName()))
			return true;
		return false;
	}
	
	@Override
	public int hashCode(){
		int hash = 7;
		hash = this.getId().hashCode() + 31*(this.getName().hashCode());
		return hash; 
	}
}
