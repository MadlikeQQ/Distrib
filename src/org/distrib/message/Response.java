package org.distrib.message;

import java.io.Serializable;
import java.util.SortedMap;

public class Response extends Message implements Serializable{
	protected static final long serialVersionUID = Message.serialVersionUID + 1L;
	private String operation = null;
	private Object payload = null;
	private String SrcNode = null;
	
	
	public Response(){
		super();
	}
	
	public Response(String operation, Object payload){
		super();
		this.operation = operation;
		this.payload = payload;
	}
	
	public String getOperation(){
		return operation;
	}
	
	public Object getPayload(){
		return payload;
	}
	
	public void setNode(String node){
		SrcNode = node;
	}
	
	public String getNode(){
		return SrcNode;
	}
	
}
