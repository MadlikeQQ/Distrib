package org.distrib.message;

import java.io.Serializable;
import java.util.SortedMap;

public class Message implements Serializable {
	
	protected static final long serialVersionUID = 1L;
	private int source;
	private int destination;
	private String id;
	
	public Message(){
		
	}
	public void addToStringProperties(SortedMap<String, Object> propertiesMap) {
		propertiesMap.put("source", source);
		propertiesMap.put("destination", destination);
	}
	public int getSource() {
		return source;
	}

	public void setSource(int source) {
		this.source = source;
	}
	
	public int getDestination() {
		return destination;
	}

	public void setDestination(int destination) {
		this.destination = destination;
	}
	
	public void setId(String id){
		this.id=id;
	}
	
	public String getId(){
		return id;
	}
	
}
	
