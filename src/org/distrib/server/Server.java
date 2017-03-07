package org.distrib.server;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.distrib.key.*;
import org.distrib.message.Request;
import org.distrib.message.Response;
import org.distrib.message.XRequest;
import org.distrib.client.*;
import org.distrib.emulator.Emulator;


public class Server extends Thread implements Runnable {

	public String myId;
	public boolean coord;
	
	public int N = 1048576;
	private static final int MIN_POOL_SIZE = 1;
	private static final int MAX_POOL_SIZE = 100;
	private static final int DEFAULT_ALIVE_TIME = 60000;
	
	private HashMap<String,String> mySet = null;
	private Tuple<String,Integer> next =null;
	private Tuple<String,Integer> previous = null;
	private MessageDigest shaGen;
	private int port = 0;
	public Emulator parent = null;
	private ThreadPoolExecutor workerThreadPool;
	private boolean hasToRun = true;
	private boolean running = false;
	private ServerSocket serverSocket;
	private CountDownLatch startSignal = new CountDownLatch(1) ;
	
	
	
	public Server(String ServerId, int port, Emulator parent) throws IOException{
		//super(port);
		this.myId = ServerId;		
		this.mySet = new HashMap<String,String>();
		this.port = port;
		this.parent = parent;
		this.coord = false;
		
		/*
		 try {
			this.shaGen = MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
	}
	
	public synchronized void start() {
		super.start();
		
		try {
			startSignal.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void createWorkerThreadPool(){
		this.workerThreadPool = new ThreadPoolExecutor(
				MIN_POOL_SIZE, 
				MAX_POOL_SIZE, 
				DEFAULT_ALIVE_TIME, 
				TimeUnit.MILLISECONDS, 
				new ArrayBlockingQueue<Runnable>(10)) ;
	}
	
	private void addShutdownHook() {
		final Server thisRef = this;
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				thisRef.shutdown();
			}
		});
	}
	
	public void shutdown() {
		
		if (running) {
			this.hasToRun = false;
			if (this.serverSocket != null) {
				try {
					this.serverSocket.close();
				} catch (IOException e) {
				}
			}
			if(workerThreadPool != null)
				workerThreadPool.shutdown();
		}
	}
	
	public int getLocalPort() {return port;}
	
	
/************************* Commands **********************/	
	
	public Tuple<String,String> insert(String key, String value){
	//	System.out.println("inserted key " + key);
		if(mySet.containsKey(key)){
			String oldval = mySet.get(key);
			//update existing value
			mySet.replace(key, oldval, value);
		}
		else{
			mySet.put(key, value);
		}
//		System.out.println("Node with Id " + myId + " has the set: " + mySet);
		return new Tuple<String,String>(key,value);
	}
	public String delete(String key){
	//	System.out.println("Node with Id " + myId  + " to remove key " + key);
		mySet.remove(key);
		return key;
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
	
	
/*************************** End commands *********************/	
	
	
	public void setNeighbors(String previous, int prev_port, String next, int next_port){
		this.next = new Tuple<String,Integer>(next,next_port);
		this.previous = new Tuple<String,Integer>(previous,prev_port);
	}

	
/**************************** Coordinator Commands *************************/	
	
	public void join (String id) throws IOException{
		//Set the new maxport for new nodes
		int port = parent.maxport+1;
		parent.maxport=port;
		Server node = new Server(id,port,parent);
		int i;
		for(i=0; i<parent.nodes.size(); i++){
			if (Key.compare(parent.nodes.get(i).myId,id)==1){
				//Set neighbors for node to be inserted
				node.setNeighbors(parent.nodes.get(i-1).myId, parent.nodes.get(i-1).port, parent.nodes.get(i).myId, parent.nodes.get(i).port);
				//Update neighbors to adjacent nodes of the new one
				parent.nodes.get(i-1).next=new Tuple<String,Integer>(id,port);
				parent.nodes.get(i).previous=new Tuple<String,Integer>(id,port);
				//Add the node to list
				parent.nodes.add(i,node);
				return;
			}
			//If we must insert to the beginning
			else if(i==0 && Key.compare(parent.nodes.get(i).myId,id)==1){			
			    //Index of last node
				int last = parent.nodes.size()-1;
				//Set the neighbors for node to be inserted
				node.setNeighbors(parent.nodes.get(last).myId, parent.nodes.get(last).port, parent.nodes.get(0).myId, parent.nodes.get(0).port);
				//Update the previous neighbor of the head node
				parent.nodes.get(0).previous=new Tuple<String,Integer>(id,port);
				//Insert the new node to head
				parent.nodes.add(0,node);
				return;
			}
		}
		int last = parent.nodes.size()-1;
		//The node must be inserted at end
		//Set the neighbors for new node
		node.setNeighbors(parent.nodes.get(last).myId, parent.nodes.get(last).port, parent.nodes.get(0).myId, parent.nodes.get(0).port);
		//Update the neighbors for the last node of list
		parent.nodes.get(last).next = new Tuple<String,Integer>(id,port);
		//Add the node to list
		parent.nodes.add(last+1,node);
		
		//2 methods for redistribution of keys: 1 for previous and 1 for next
	}
	
	public void depart (String id){
		int i;
		for(i=0; i<parent.nodes.size(); i++){
			if (parent.nodes.get(i).myId.equals(id)){
				//We must remove the first node from the list
				if(i==0){
					parent.nodes.get(i+1).previous=parent.nodes.get(i).previous;
				}
				//We must remove the last node from the list
				else if (i == parent.nodes.size()-1){
					parent.nodes.get(i-1).next=parent.nodes.get(i).next;
					
				}
				//We must remove an intermediate node from the list
				else{
					parent.nodes.get(i-1).next=parent.nodes.get(i).next;
					parent.nodes.get(i).previous=parent.nodes.get(i).previous;
					
				}
				//1 methods for redistribution: next gets all keys
				parent.nodes.remove(i);
		}
	}
}
	
/**************************** End Coordinator Commands *********************/	
	
	
	
	private synchronized void incCounter (){
		parent.counter++;
	}
	private synchronized void resetCounter(){
		parent.counter = 0;
	}
	
	public void run()
	{	
		running = true;
		try {
			serverSocket = new ServerSocket(this.port);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		createWorkerThreadPool();
		addShutdownHook();
		
		startSignal.countDown();
		
		while(hasToRun)
		{
			Socket socket = null;
			try {
				socket = serverSocket.accept();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			DoWork w = new DoWork(socket);
			this.workerThreadPool.submit(w);
			//new Thread(new DoWork(socket)).start();
			
		}
		
		running = false;
	}
	
	
	
	private class DoWork implements Runnable {
		
		Socket socket = null;
		public DoWork(Socket socket){
			this.socket = socket;
		}

		/**************************** Handle Requests *****************************/
		public void HandleRequests(Request req) throws IOException{
		//	System.out.println("received req with src" + req.getSource());
			int i, comp ;
			String key, value, respondPort, keySHA;
			String operation = req.getOperation();
			String operands = req.getOperands();
			switch (operation){
			case "insert":
				i = operands.indexOf(',');
				key = operands.substring(1, i) ;
				value = operands.substring(i+2,operands.length() );
				//System.out.println("Received insert <" + key + "," + value + ">");
				//comp = compareKeys(myId,key) ;
			//	keySHA = Key.generate(key, N);
				keySHA = Key.sha1(key);
				comp = Key.compare(myId, keySHA);
				if ( comp == 1 || comp == 0)
				{
					if( Key.compare(previous.a, keySHA) == -1 ){
						Tuple<String,String> result = insert(key,value);
						int src = req.getSource();
						Response response = new Response("insert",result);
						response.setDestination(src);
						response.setSource(port);
						response.setNode(myId);
						//send response to client
						new Thread(new Client("127.0.0.1", src, response)).start();
					}
					else {
						new Thread( new Client("127.0.0.1", next.b, req)).start();
						//send insert command to previous 
					}
				}
				else if (comp == -1) {
					//send insert to next
					if (!myId.equals(parent.nodes.get(parent.nodes.size()-1).myId))
						new Thread( new Client("127.0.0.1", next.b, req)).start();
					else{
						XRequest xreq = (XRequest)req;
						xreq.setType("mandatory");
						new Thread( new Client("127.0.0.1", next.b, xreq)).start();
					}
				}
				break;
			case "delete":
				key = operands.substring(1, operands.length()) ;
				//System.out.println("received delete command for: " + key);
			//	keySHA = Key.generate(key, N);
				keySHA = Key.sha1(key);
				comp = Key.compare(myId,keySHA) ;
				if ( comp == 1 || comp == 0)
				{
					if( Key.compare(previous.a, keySHA) == -1 ){
					    String result = delete(key);
						int src = req.getSource();
						Response response = new Response("delete",result);
						response.setDestination(src);
						response.setSource(port);
						response.setNode(myId);
						//System.out.println("Sendin response to node "+ src+ "for key " + result);
						//send response to client
						new Thread(new Client("127.0.0.1", src, response)).start();
					}
					else {
						new Thread( new Client("127.0.0.1", next.b, req)).start();
						//send delete command to previous 
					}
				}
				else if (comp == -1) {
					if (!myId.equals(parent.nodes.get(parent.nodes.size()-1).myId))
						new Thread( new Client("127.0.0.1", next.b, req)).start();
					else{
						XRequest xreq = (XRequest)req;
						xreq.setType("mandatory");
						new Thread( new Client("127.0.0.1", next.b, xreq)).start();
					}
					//send delete to next
				}
				break;
			case "query":
				key = operands.split(" ")[1] ;
				//comp = compareKeys(myId,key) ;
				//keySHA = Key.generate(key, N);
				keySHA = Key.sha1(key);
				comp = Key.compare(myId, keySHA);
				if(key.equals("*")){
					incCounter();
					if(parent.counter <= parent.NUM_NODES) {
						ArrayList<Tuple<String,String>> result = new ArrayList<Tuple<String,String>>();
						result = query(key);
						int src = req.getSource();
						Response response = new Response("query",result);
						response.setDestination(src);
						response.setSource(port);
						response.setNode(myId);
						//send response to client
						new Thread(new Client("127.0.0.1", src, response)).start();
						//send message to next
						new Thread( new Client("127.0.0.1", next.b, req)).start();
						//send answer to first client
						//System.out.println("Set :" + mySet);
						//new Thread( new Client("127.0.0.1", Integer.parseInt(respondPort), command)).start();
					}
					else 
						resetCounter();
					break;
				}
				if ( comp == 1 || comp == 0)
				{
					System.out.println("Normal query");
					if( Key.compare(previous.a, keySHA) == -1 ){
						System.out.println("I have key " + myId);
						ArrayList<Tuple<String,String>> result = new ArrayList<Tuple<String,String>>();
						result = query(key);
						int src = req.getSource();
						Response response = new Response("query",result);
						response.setDestination(src);
						response.setSource(port);
						response.setNode(myId);
						//send response to client
						new Thread(new Client("127.0.0.1", src,response)).start();
					}
				}
				//Send to next node
				else{
					if (!myId.equals(parent.nodes.get(parent.nodes.size()-1).myId))
						new Thread( new Client("127.0.0.1", next.b, req)).start();
					else{
						XRequest xreq = (XRequest)req;
						xreq.setType("mandatory");
						new Thread( new Client("127.0.0.1", next.b, xreq)).start();
					}
				}
				break;
			case "show":
				System.out.println("show");
				for(int k=0; k < parent.nodes.size(); k++){
		    		System.out.println("Node: " + Key.toHex(parent.nodes.get(k).myId)+" with port: "+ parent.nodes.get(k).getLocalPort());
		    	}
				break;
			case "join":
				if (coord ==true){
					//i = operands.indexOf(',');
					String id = operands.split(" ")[1] ;
			//		keySHA = Key.generate(id, N);
					keySHA = Key.sha1(id);
					join(keySHA);
				}
				break;
			case "depart":
				if (coord ==true){
					//i = operands.indexOf(',');
					String id = operands.split(" ")[1] ;
				//	System.out.println("Depart of node"+id+"blalblaldfl");
					keySHA = Key.sha1(id);
			//		System.out.println("With sha "+keySHA);
					depart(keySHA);
				}
				break;
			}
		}
		
		public void HandleXRequests(XRequest xreq){
			int i, src ;
			String key, value, keySHA;
			String operation = xreq.getOperation();
			String operands = xreq.getOperands();
			String type = xreq.getType();
			Response response=null;
			if(type.equals("mandatory")){
				switch (operation){
				case "insert":
					//System.out.println("Received insert");
					i = operands.indexOf(',');
					key = operands.substring(1, i) ;
					value = operands.substring(i+2,operands.length() );
					//comp = compareKeys(myId,key) ;
					//	keySHA = Key.generate(key, N);
					keySHA = Key.sha1(key);
					Tuple<String,String> ires = insert(key,value);
					src = xreq.getSource();
					response = new Response("insert",ires);
					response.setDestination(src);
					response.setSource(port);
					response.setNode(myId);
					//send response to client
					new Thread(new Client("127.0.0.1", src, response)).start();
					break;
				case "delete":
					i = operands.indexOf(',');
					key = operands.substring(1, i) ;
					//	keySHA = Key.generate(key, N);
					keySHA = Key.sha1(key);
					String dres = delete(key);
					src = xreq.getSource();
					response = new Response("delete",dres);
					response.setDestination(src);
					response.setSource(port);
					response.setNode(myId);
					System.out.println("Sendin response to node "+ src+ "for key " + dres);
					//send response to client
					new Thread(new Client("127.0.0.1", src, response)).start();
					break;
				case "query":
					key = operands.split(" ")[1] ;
					//comp = compareKeys(myId,key) ;
					//keySHA = Key.generate(key, N);
					keySHA = Key.sha1(key);
					ArrayList<Tuple<String,String>> qres = new ArrayList<Tuple<String,String>>();
					qres = query(key);
					src = xreq.getSource();
					response = new Response("query",qres);
					response.setDestination(src);
					response.setSource(port);
					response.setNode(myId);
					//send response to client
					new Thread(new Client("127.0.0.1", src,response)).start();
				}
		}	
	}
		
		private void HandleResponse(Response response){
			String operation = response.getOperation();
			Object payload = response.getPayload();
			System.out.println("*****Reponse******");
			System.out.println("From: " + Key.toHex(response.getNode()));
			System.out.println("To  : " + Key.toHex(myId));
			System.out.println("Message: ");
			switch (operation){
			case "query":
				ArrayList<Tuple<String,String>> pld = (ArrayList<Tuple<String,String>>)payload;
				//System.out.println("Data from node " + response.getNode()+ ":");
				for(int i=0; i<pld.size(); i++){
					System.out.println(pld.get(i).a + ","+pld.get(i).b);
				}
				
				break;
			case "insert":
				Tuple<String,String> insrt = (Tuple<String,String>)payload;
				System.out.println("Inserted <" + insrt.a + "," + insrt.b + ">");
				break;
			case "delete":
				String dlt = payload.toString();
				System.out.println("Deleted key "+ dlt  );
			break;
			}		
			System.out.println();
		}
		
/*********************************** End Handle Requests/Responses ***************************/		
		
		
		
		@Override
		public void run(){
			ObjectInputStream in = null;
			try{
				in = new ObjectInputStream( socket.getInputStream());
				Object messageObj =  null;
				messageObj = in.readObject();
				//System.out.println(messageObj.getClass());
				if(messageObj instanceof Request){
					HandleRequests((Request) messageObj);
				}
				else if(messageObj instanceof Response){
					HandleResponse((Response) messageObj);
				}
				else{
					HandleXRequests((XRequest)messageObj);
				}

			//	in = new BufferedReader(inS);
			} catch (IOException e) {
				// TODO Auto-generated catch blockσ
				e.printStackTrace();
				System.out.print("IOEXCP");
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			finally{//close resources
				try {
					if(in != null)
						in.close();
					if(socket != null)
						socket.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}

	}
	
}
