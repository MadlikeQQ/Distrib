package org.distrib.server;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;


public class Server extends ServerSocket {

	public String myId;
	private HashMap<String,String> mySet = null;
	private Tuple<String,Integer> next =null;
	private Tuple<String,Integer> previous = null;
	private MessageDigest shaGen;
	
	public Server(String ServerId, int port) throws IOException{
		super(port);
		this.myId = ServerId;		
		this.mySet = new HashMap<String,String>();
		try {
			this.shaGen = MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
	public void delete(String key){
		mySet.remove(key);
	}
	
	public ArrayList<Tuple<String,String>> query(String key){
		ArrayList<Tuple<String,String>> res = new ArrayList<Tuple<String,String>>();
		if(key.equals("*")){
			//lambda: for each (k,v) pair in mySet, add a new Tuple to res 
			mySet.forEach((k,v) -> res.add(new Tuple<String,String>(k,v)));
		}
		else{
			if(mySet.containsKey(key))
				res.add(new Tuple<String,String>(key,mySet.get(key)));
		}
		return res;
	}
	
	public void setNeighbors(String previous, int prev_port, String next, int next_port){
		this.next = new Tuple<String,Integer>(next,next_port);
		this.previous = new Tuple<String,Integer>(previous,prev_port);
	}
	
	public void join (String previous){}
	public void depart (String id){}
	
	private int compareKeys(String nodeID, String key){
		byte[] sha1 = nodeID.getBytes(); 

		shaGen.update(key.getBytes());
		byte[] sha2 = shaGen.digest();

		for (int i = 0; i < sha1.length; i++ ){
			if(sha1[i] < sha2[i]) return -1;
			else if (sha1[i] > sha2[i]) return 1;
		}
		return 0;
	}
	//main loop server listens for messages
	public static void main(String[] args) throws IOException{
		Server s = new Server("iuef877wieu", 4444);

		while(true)
		{
			Socket client = s.accept();
			try{
				//PrintWriter out = new PrintWriter(client.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream(),"UTF8"));
				String command ;
				while( (command = in.readLine()) != null ){
					String operation = command.substring(0, command.indexOf(','));
					String operands = command.substring(command.indexOf(',') + 1, command.length() ).trim();
					System.out.println(command);
					System.out.println(operation);
					System.out.println(operands);
					int i, comp ;
					String key, value;
					switch (operation){
					case "insert":
						i = operands.indexOf(',');
						key = operands.substring(0, i) ;
						value = operands.substring(i+2,operands.length() - 1 );
						comp = s.compareKeys(s.myId,key) ;
						if ( comp == 1 || comp == 0)
						{
							if( s.compareKeys(s.previous.a, key) == -1 ){
								s.insert(key,value);
							}
							else {
								//send insert command to previous 
							}
						}
						else if (comp == -1) {
							//send insert to next
						}
						break;
					case "delete":
						key = operands;
						comp = s.compareKeys(s.myId,key) ;
						if ( comp == 1 || comp == 0)
						{
							if( s.compareKeys(s.previous.a, key) == -1 ){
								s.delete(key);
							}
							else {
								//send delete command to previous 
							}
						}
						else if (comp == -1) {
							//send delete to next
						}
						break;
					case "query":
						key = operands;
						comp = s.compareKeys(s.myId,key) ;
						if(key.equals("*")){
							//counter++
							//if counter < totalProcesses
							s.query(key);
							//send message to next
							//else counter = 0
							break;
						}
						if ( comp == 1 || comp == 0)
						{
							if( s.compareKeys(s.previous.a, key) == -1 ){
								s.query(key);
							}
							else {
								//send queery command to previous 
							}
						}
						else if (comp == -1) {
							//send query to next
						}
						break;
					}
					
				}
			}
			finally{
				client.close();
			}
		}
	}
	
}
