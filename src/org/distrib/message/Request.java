package org.distrib.message;

import java.io.Serializable;
import java.util.SortedMap;

public class Request extends Message implements Serializable {
	protected static final long serialVersionUID = Message.serialVersionUID + 1L;
	private String operation = "";
	private String operands = "";
	
	
	public Request() {
		super();
		//setConversationId(getNextConversationId());
	}
	
	public Request(String command) {
		super();
		setCommand(command);
		//setConversationId(getNextConversationId());
	}
	
	public void setCommand(String command){
		int i = command.indexOf(',');
		if(i>0){
		operation = command.substring(0, i);
		operands = command.substring(i + 1 , command.length());
		}
	}
	
	public String getOperation(){
		return operation;
	}
	
	public String getOperands(){
		return operands;
	}
}
