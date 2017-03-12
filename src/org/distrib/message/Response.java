package org.distrib.message;

import java.io.Serializable;

public class Response extends Message implements Serializable{
	protected static final long serialVersionUID =  1L;
	private String operation = null;
	private Object payload = null;
	private String SrcNode = null;
	private String command = null;
	private long originalRequestID;
	private long timeOriginated;
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
	
	public void setPayload(Object obj){
		payload = obj;
	}
	
	public void setNode(String node){
		SrcNode = node;
	}
	
	public String getNode(){
		return SrcNode;
	}
	
	public void setCommand(String command){
		this.command = command;
	}
	
	public void setOriginalRequestID(long ID){
		this.originalRequestID = ID;
	}
	
	public long getOriginalRequestID(){
		return this.originalRequestID;
	}
	
	public String getCommand(){
		return this.command;
	}
	
	public long getTimeOriginated()
	{return this.timeOriginated;}
	public void setTimeOriginated(long t){this.timeOriginated =t;}
	
}
