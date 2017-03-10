package org.distrib.message;
public class XRequest extends Request {
	
	/**
	 * 
	 */
	private static long serialVersionUID = 1L;
	private String type =null;
	private int k = 0;
	
	
	public XRequest (String type,String command){
		super(command);
		this.type = type;
	}
	
	public void setType(String type){
		this.type = type;
	}
	
	public String getType(){
		return this.type;
	}
	
	public void setK(int k){
		this.k = k;
	}
	
	public void decK(){
		--k;
	}
	
	public int getK(){
		return k;
	}
	

}
