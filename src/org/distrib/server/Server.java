package org.distrib.server;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

public class Server extends ServerSocket {

	public String myId;
	private HashMap<String,String> mySet = null;
	private Tuple next =null;
	private Tuple previous = null;
	
	public Server(String ServerId, int port) throws IOException{
		super(port);
		this.myId = ServerId;		
		this.mySet = new HashMap<String,String>();
	}
	
	public void insert(String key, String value){
		if(mySet.containsKey(key)){
			String oldval = mySet.get(key);
			//update existing value
			mySet.replace(key, oldval, value);
		}
		else{
			mySet.put(key, value);

		}
	}
	public void remove(String key, String value){}
	
	public HashMap<String,String> query(String key){}
	
	public void setNeighbors(String previous, int prev_port, String next, int next_port){
		this.next = new Tuple(next,next_port);
		this.previous = new Tuple(previous,prev_port);
	}
	
	public void join (String previous)
}
