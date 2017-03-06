package org.distrib.message;

public class XRequest extends Request {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String type =null;
	
	public XRequest (String type,String command){
		super(command);
	}
	
	public void setType(String type){
		this.type = type;
	}
	
	public String getType(){
		return this.type;
	}

}
