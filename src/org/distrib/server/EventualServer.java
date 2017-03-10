package org.distrib.server;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.distrib.key.*;
import org.distrib.message.Request;
import org.distrib.message.Response;
import org.distrib.message.XRequest;
import org.distrib.client.*;
import org.distrib.emulator.Emulator;


public class EventualServer extends Thread implements Runnable {

	public String myId;
	public boolean coord;
	
	public int N = 1048576;
	private static final int MIN_POOL_SIZE = 1;
	private static final int MAX_POOL_SIZE = 200;
	private static final int DEFAULT_ALIVE_TIME = 60000;
	private HashMap<String,Tuple<String,Integer>> mySet = null;
	private Tuple<String,Integer> next =null;
	private Tuple<String,Integer> previous = null;
	private int port = 0;
	public Emulator parent = null;
	private ExecutorService workerThreadPool;
	private boolean hasToRun = true;
	private boolean running = false;
	public ServerSocket serverSocket;
	private CountDownLatch startSignal = new CountDownLatch(1) ;
	private int K=3 ;
	private int departIdx = -1;
	
	
	public EventualServer(String ServerId, int port, Emulator parent) throws IOException{
		this.myId = ServerId;		
		this.mySet = new HashMap<String,Tuple<String,Integer>>();
		this.port = port;
		this.parent = parent;
		this.coord = false;
	}
	
	public EventualServer(String ServerId, int port, Emulator parent, int K) throws IOException{
		this.myId = ServerId;		
		this.mySet = new HashMap<String,Tuple<String,Integer>>();
		this.port = port;
		this.parent = parent;
		this.coord = false;
		this.K = K;
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
	 
	public synchronized  void queueShutdown()
	{
		this.hasToRun = false;
	}
	
	 
	public void setTimeout(int t){
		try {
			this.serverSocket.setSoTimeout(t);
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void createWorkerThreadPool(){
		this.workerThreadPool = Executors.newFixedThreadPool(MAX_POOL_SIZE);
				/*new ThreadPoolExecutor(
				MIN_POOL_SIZE, 
				MAX_POOL_SIZE, 
				DEFAULT_ALIVE_TIME, 
				TimeUnit.MILLISECONDS, 
				new ArrayBlockingQueue<Runnable>(10)) ;*/
	}
	
	private void addShutdownHook() {
		final EventualServer thisRef = this;
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				thisRef.shutdown();
			}
		});
	}
	
	public void shutdown() {
		
		if (running) {
			this.hasToRun = false;
			if(workerThreadPool != null)
				workerThreadPool.shutdown();
			
			if (this.serverSocket != null) {
				try {
					this.serverSocket.close();
				} catch (IOException e) {
				}
			}

		}
	}
	 
	public int getLocalPort() {return port;}
	
/************************* Commands **********************/	
	
	 
	public Tuple<String,String> insert(String key, String value, int k ){
		if(mySet.containsKey(key)){
			Tuple<String,Integer> oldval = mySet.get(key);
			mySet.replace(key, oldval, new Tuple<String,Integer>(value,k));
		}
		else{
			mySet.put(key, new Tuple<String,Integer>(value,k));
		}
		return new Tuple<String,String>(key,value);
	}
	
	
	 
	public ArrayList<Tuple<String,Tuple<String,Integer>>> insertBatch(ArrayList<Tuple<String,Tuple<String,Integer>>> batch){
		Iterator<Tuple<String, Tuple<String,Integer>>> it = batch.iterator();
		Tuple<String, Tuple<String,Integer>> temp;
		while(it.hasNext()){
			temp = it.next();
			if(temp.b.b>=0){
				mySet.put(temp.a, new Tuple<String,Integer>(temp.b.a,temp.b.b));
			}
		}
		ArrayList<Tuple<String,Tuple<String,Integer>>> ret = new ArrayList<Tuple<String,Tuple<String,Integer>>>();
		it = batch.iterator();
		while(it.hasNext()){
			temp = it.next();
			temp.b.b++;
			if(K - temp.b.b != 0){
				ret.add(temp);
			}
		}
		
		return ret;
	}
	
	 
	public ArrayList<Tuple<String,Tuple<String,Integer>>> joinBatch(ArrayList<Tuple<String,Tuple<String,Integer>>> batch){
		Iterator<Tuple<String, Tuple<String,Integer>>> it = batch.iterator();
		Tuple<String, Tuple<String,Integer>> temp;
		while(it.hasNext()){
			temp = it.next();
			if(temp.b.b<K){
				mySet.put(temp.a, new Tuple<String,Integer>(temp.b.a,temp.b.b));
			}
			else{
				mySet.remove(temp.a);
			}
		}
		ArrayList<Tuple<String,Tuple<String,Integer>>> ret = new ArrayList<Tuple<String,Tuple<String,Integer>>>();
		it = batch.iterator();
		while(it.hasNext()){
			temp = it.next();
			temp.b.b++;
			if(K - temp.b.b >= 0){
				ret.add(temp);
			}
		}
		return ret;
	}
	
	
	 
	public String delete(String key){
		mySet.remove(key);
		return key;
	}
	
	 
	public ArrayList<Tuple<String,String>> query(String key){
		ArrayList<Tuple<String,String>> res = new ArrayList<Tuple<String,String>>();
		if(key.equals("*")){
			mySet.forEach((k,v) -> res.add(new Tuple<String,String>(k,v.a)));
		}
		else{
			if(mySet.containsKey(key))
				res.add(new Tuple<String,String>(key,mySet.get(key).a));
		}
		return res;
	}
	
	 
	public ArrayList<Tuple<String, Tuple<String,Integer>>> getSet(String key){
		ArrayList<Tuple<String, Tuple<String,Integer>>> ret;
		ret = new ArrayList<Tuple<String, Tuple<String,Integer>>>() ;
		if(key.equals("*")){
			mySet.forEach((k,v) -> ret.add(new Tuple<String,Tuple<String,Integer>>(k,v)));
		}
		else{
			Tuple<String,Integer> val =  mySet.get(key);
			if(val != null)
				ret.add(new Tuple<String,Tuple<String,Integer>>(key,val));
		}
		return ret;
	}
	
	 
	public ArrayList<Tuple<String, Tuple<String,Integer>>> getRightKeys(){
		ArrayList<Tuple<String, Tuple<String,Integer>>> ret = new ArrayList<Tuple<String, Tuple<String,Integer>>>();
		mySet.forEach((k,v) -> {
			String keySHA = Key.sha1(k);
			if(v.b > 0){
				ret.add(new Tuple<String,Tuple<String,Integer>>(k,v));
			}
			else if (Key.compare(previous.a, keySHA) != -1){
					ret.add(new Tuple<String,Tuple<String,Integer>>(k,v)); 
				}
		});
		return ret;
	}
	
	
/*************************** End commands *********************/	
	
	public void setNeighbors(String previous, int prev_port, String next, int next_port){
		this.next = new Tuple<String,Integer>(next,next_port);
		this.previous = new Tuple<String,Integer>(previous,prev_port);
	}

	
/**************************** Coordinator Commands *************************/	
	
	 
	public XRequest join (String id) throws IOException{
		parent.NUM_NODES++;
		//Set the new maxport for new nodes
		int port = parent.maxport+1;
		parent.maxport=port;
		EventualServer node = new EventualServer(id,port,parent,K);
		int i;
		for(i=0; i<parent.nodes.size(); i++){
			if (i!= 0 && Key.compare(parent.nodes.get(i).myId,id)==1){
				//Set neighbors for node to be inserted
				node.setNeighbors(parent.nodes.get(i-1).myId, parent.nodes.get(i-1).port, parent.nodes.get(i).myId, parent.nodes.get(i).port);
				//Update neighbors to adjacent nodes of the new one
				parent.nodes.get(i-1).next=new Tuple<String,Integer>(id,port);
				parent.nodes.get(i).previous=new Tuple<String,Integer>(id,port);
				//Add the node to list
				parent.nodes.add(i,node);
				break;
			}
			//If we must insert to the beginning
			else if(i==0 && Key.compare(parent.nodes.get(i).myId,id)==1){			
			    //Index of last node
				int last = parent.nodes.size()-1;
				//Set the neighbors for node to be inserted
				node.setNeighbors(parent.nodes.get(last).myId, parent.nodes.get(last).port, parent.nodes.get(0).myId, parent.nodes.get(0).port);
				//Update the previous neighbor of the head node
				parent.nodes.get(0).previous=new Tuple<String,Integer>(id,port);
				parent.nodes.get(last).next =new Tuple<String,Integer>(id,port);
				//Insert the new node to head
				parent.nodes.add(0,node);
				break;
			}
		}
		if(i == parent.nodes.size()){
			int last = parent.nodes.size()-1;
			//The node must be inserted at end
			//Set the neighbors for new node
			node.setNeighbors(parent.nodes.get(last).myId, parent.nodes.get(last).port, parent.nodes.get(0).myId, parent.nodes.get(0).port);
			//Update the neighbors for the last node of list
			parent.nodes.get(last).next = new Tuple<String,Integer>(id,port);
			parent.nodes.get(0).previous = new Tuple<String,Integer>(id,port);
			//Add the node to list
			parent.nodes.add(i,node);
		}
		XRequest xreq = new XRequest("join","redistribute");
		xreq.setSource(port);
		xreq.setDestination(node.next.b);
		node.start();
		return xreq;
	} 
	public void departPointers(int i){
		//System.out.println("departPointers");
		parent.NUM_NODES--;
		//We must remove the first node from the list
		if(i==0){
			parent.nodes.get(i+1).previous=parent.nodes.get(i).previous;
			//set the next node as coordinator
			parent.nodes.get(i+1).coord = true;
			parent.coord_port = parent.nodes.get(i+1).getLocalPort();
		//	System.out.println("Node with port " + parent.nodes.get(i+1).getLocalPort() +" has coord val "+ parent.nodes.get(i+1).coord);
			coord =false;
			hasToRun =false;
		}
		//We must remove the last node from the list
		else if (i == parent.nodes.size()-1){
			parent.nodes.get(i-1).next=parent.nodes.get(i).next;
			
		}
		//We must remove an intermediate node from the list
		else{
			parent.nodes.get(i-1).next=parent.nodes.get(i).next;
			parent.nodes.get(i+1).previous=parent.nodes.get(i).previous;
			
		}
		//System.out.println("Removing " + i);
		parent.nodes.get(i).queueShutdown();
		Request shutdown = null;
		//send msg to node for termination
		new Thread(new Client("127.0.0.1",parent.nodes.get(i).getLocalPort(),shutdown)).start();
		parent.nodes.remove(i);
	}
	public XRequest depart (String id){
		int i;
		XRequest departReq = null;
		for(i=0; i<parent.nodes.size(); i++){
			if (parent.nodes.get(i).myId.equals(id)){
				departReq = new XRequest("depart","redistribute");
				departReq.setSource(port);
				departReq.setDestination(parent.nodes.get(i).port);
				this.departIdx = i;
				//System.out.println("depart sending request to port " + this.next.b + "with final destination " + parent.nodes.get(i).port);
				break;
		}
	}
		return departReq;
		
}
	
/**************************** End Coordinator Commands *********************/	
	
	
	
	private synchronized void incCounter (){
		parent.counter++;
	}
	private synchronized void resetCounter(){
		parent.counter = 0;
	}
	
	private synchronized void incBarrier(){
		parent.barrier++;
	}
	private synchronized void decBarrier(){
		parent.barrier--;
	}
	@Override
	public void run()
	{	
		running = true;
		try {
			serverSocket = new ServerSocket(this.port);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
		}
		createWorkerThreadPool();
		addShutdownHook();
		
		startSignal.countDown();
		
		
		
		while(hasToRun)
		{
			Socket socket = null;
			try {
				decBarrier();
				socket = serverSocket.accept();
				incBarrier();
				DoWork w = new DoWork(socket);
				this.workerThreadPool.submit(w);
			} catch (IOException e) {
			} 
			//new Thread(new DoWork(socket)).start();
			
		}
		System.out.println("Node "+ myId + " shutting down gracefully");
		shutdown();
		running = false;
	
	}
	
	
	
	private class DoWork implements Runnable {
		
		Socket socket = null;
		
		public DoWork(Socket socket){
			this.socket = socket;
		}

		/**************************** Handle Requests *****************************/
		public void HandleRequests(Request req) throws IOException{
			//System.out.println("received req with Serial ID " + req.getSerialVersionID());
			int i, comp , src;
			String key, value, keySHA;
			String operation = req.getOperation();
			String operands = req.getOperands();
			ArrayList<Tuple<String,Tuple<String,Integer>>> result = new ArrayList<Tuple<String,Tuple<String,Integer>>>();
			switch (operation){
			case "insert":
				i = operands.indexOf(',');
				key = operands.substring(1, i) ;
				value = operands.substring(i+2,operands.length() );
				keySHA = Key.sha1(key);
				result = new ArrayList<Tuple<String,Tuple<String,Integer>>>();
				result = getSet(key);
				
				Tuple<String,String> Result;
				if(Key.between(keySHA, previous.a, myId) ){
					if(K-1 > 0){
						XRequest xr = new XRequest("mandatory",req.getCommand());
						xr.setK(K-1);
						xr.setSource(port);
						new Thread(new Client("127.0.0.1", next.b, xr)).start();
					}
					
					Result = insert(key,value,0);
					src = req.getSource();
					Response response = new Response("insert",Result);
					response.setDestination(src);
					response.setSource(port);
					response.setNode(myId);
					response.setCommand(req.getCommand());
					response.setOriginalRequestID(req.getSerialVersionID());
					response.setTimeOriginated(System.currentTimeMillis() - parent.startT);
					//send response to client
					new Thread(new Client("127.0.0.1", src, response)).start();					
				}
				else {
					new Thread( new Client("127.0.0.1", next.b, req)).start();
				}
				
				break;
			case "delete":
				key = operands.substring(1, operands.length()) ;
				keySHA = Key.sha1(key);
				result = getSet(key);
				if( !result.isEmpty() && Key.between(keySHA, previous.a, myId) ){
					//Replication request
					if(K-1 > 0){
						XRequest xr = new XRequest("mandatory",req.getCommand());
						xr.setSource(port);
						xr.setK(K-1);
						new Thread(new Client("127.0.0.1", next.b, xr)).start();
					}
					delete(key);
					src = req.getSource();
					Response response = new Response("delete",key);
					response.setDestination(src);
					response.setSource(port);
					response.setNode(myId);
					response.setCommand(req.getCommand());
					response.setOriginalRequestID(req.getSerialVersionID());
					response.setTimeOriginated(System.currentTimeMillis() - parent.startT);
					//send response to client
					new Thread(new Client("127.0.0.1", src, response)).start();
				}
				else if(!Key.between(keySHA, previous.a, myId)) {
					System.out.println("Forwarding delete to next and previous is "+ previous.b);
					new Thread( new Client("127.0.0.1", next.b, req)).start();
				}
				break;
			case "query":
				key =  operands.substring(1, operands.length()) ;
				keySHA = Key.sha1(key);
				comp = Key.compare(myId, keySHA);
				
				if(key.equals("*")){
					incCounter();
					if(parent.counter <= parent.NUM_NODES) {
						result = getSet("*");//new ArrayList<Tuple<String,String>>();
						src = req.getSource();
						Response response = new Response("query",result);
						response.setDestination(src);
						response.setSource(port);
						response.setNode(myId);
						response.setCommand(req.getCommand());
						response.setOriginalRequestID(req.getSerialVersionID());
						response.setTimeOriginated(System.currentTimeMillis() - parent.startT);
						//send response to client
						new Thread(new Client("127.0.0.1", src, response)).start();
						//send message to next
						new Thread( new Client("127.0.0.1", next.b, req)).start();
					}
					else 
						resetCounter();
					break;
				}
				result = getSet(key);
				if(!result.isEmpty() ){
					src = req.getSource();
					Response response = new Response("query",result);
					response.setDestination(src);
					response.setSource(port);
					response.setNode(myId);
					response.setCommand(req.getCommand());
					response.setOriginalRequestID(req.getSerialVersionID());
					response.setTimeOriginated(System.currentTimeMillis() - parent.startT);
					//send response to client
					new Thread(new Client("127.0.0.1", src,response)).start();
				}
				else if (!Key.between(keySHA, previous.a, myId)) {
					new Thread( new Client("127.0.0.1", next.b, req)).start();
					//send delete command to previous 
				}
				else if (Key.between(keySHA, previous.a, myId) && result.isEmpty()){
					src = req.getSource();
					Response response = new Response("query",result);
					response.setDestination(src);
					response.setSource(port);
					response.setNode(myId);
					response.setCommand(req.getCommand());
					response.setOriginalRequestID(req.getSerialVersionID());
					response.setTimeOriginated(System.currentTimeMillis() - parent.startT);
					//send response to client
					new Thread(new Client("127.0.0.1", src,response)).start();
				}
				break;
			case "show":
				for(int k=0; k < parent.nodes.size(); k++){
		    		System.out.println("Node: " + (parent.nodes.get(k).myId)+" with port: "+ parent.nodes.get(k).getLocalPort());
		    	}
				break;
			case "join":
				if (coord ==true){
					String id = operands.substring(1, operands.length()) ;
					keySHA = Key.sha1(id);
					XRequest xreq =	null;
					xreq= join(keySHA);
					new Thread(new Client("127.0.0.1",next.b,xreq)).start();
				}
				break;
			case "depart":
				if (coord ==true){
					String id = operands.substring(1, operands.length()) ;
					keySHA = Key.sha1(id);
					XRequest wtf = depart(keySHA);
					new Thread(new Client("127.0.0.1",next.b,wtf)).start();
				}
				break;
			}
		}
		
		public void HandleXRequests(XRequest xreq){
			int i, src,k ;
			String key, value, keySHA;
			String operation = xreq.getOperation();
			String operands = xreq.getOperands();
			String type = xreq.getType();
			

			Response response=null;
			if(type.equals("mandatory")){
				switch (operation){
				case "insert":
					k = xreq.getK();
					if(k-1 >= 0){
						i = operands.indexOf(',');
						key = operands.substring(1, i) ;
						value = operands.substring(i+2,operands.length() );
						keySHA = Key.sha1(key);
						Tuple<String,String> ires = insert(key,value, K - k);
						//Replica
						xreq.setK(k-1);
						xreq.setDestination(next.b);
						new Thread(new Client("127.0.0.1", next.b, xreq)).start();
					}
					break;
				case "delete":
					k = xreq.getK();
					key=operands.substring(1);
					ArrayList<Tuple<String,Tuple<String,Integer>>> result = new ArrayList<Tuple<String,Tuple<String,Integer>>>();
					result = getSet(key);
					if(k-1 >= 0){
						String dres = delete(key);
						src = xreq.getSource();
						//Replica
						xreq.setK(k-1);
						new Thread(new Client("127.0.0.1", next.b, xreq)).start();
					}
					break;
				case "query":
					key = operands.split(" ")[1] ;
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
			else if(type.equals("depart")){
				operation = xreq.getCommand();
				switch(operation){
				case "redistribute":
					if(xreq.getDestination() == port){
						//i received an XRequest to give my keys (depart)
						ArrayList<Tuple<String,Tuple<String, Integer>>> result = getSet("*");
						Response sendKeys = new Response("insertbatch", result);
						sendKeys.setSource(xreq.getSource());
						sendKeys.setDestination(previous.b);
						sendKeys.setNode(myId);
						new Thread(new Client("127.0.0.1", next.b,sendKeys)).start();
						hasToRun = false;
					}
					else{
						//i received an XRequest (not for me) 
						new Thread(new Client("127.0.0.1",next.b,xreq)).start();
					}
				break;
				}
			}
			else if (type.equals("join")){
				operation = xreq.getCommand();
				if(xreq.getDestination() == port){
					ArrayList<Tuple<String,Tuple<String, Integer>>> result = new ArrayList<Tuple<String,Tuple<String, Integer>>>();
					result = getRightKeys();
					Response sendKeys = new Response("joinbatch", result);
					sendKeys.setSource(xreq.getSource());
					sendKeys.setDestination(previous.b);
					sendKeys.setNode(myId);
					new Thread(new Client("127.0.0.1", next.b,sendKeys)).start();
				}
				else{
					new Thread(new Client("127.0.0.1",next.b,xreq)).start();
				}
			}
	}
		private void printMessage(Response response){
			System.out.println("*****Reponse " + response.getOperation() + "******");
			System.out.println("From: " + (response.getNode()));
			System.out.println("To  : " + (myId));
			System.out.println("Request: " + response.getCommand() + " ID " + response.getOriginalRequestID());
			System.out.println("Operation was done at : " + response.getTimeOriginated());
			System.out.println("Message: ");
		}
		

		private void HandleResponse(Response response){
			String operation = response.getOperation();
			Object payload = response.getPayload();
			try {
				parent.printLatch.acquire();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			switch (operation){
			case "query":
				printMessage( response);
				ArrayList<Tuple<String,Tuple<String,Integer>>> pld = (ArrayList<Tuple<String,Tuple<String,Integer>>>)payload;
				for(int i=0; i<pld.size(); i++){
					System.out.println(pld.get(i).a + ","+pld.get(i).b.a + "," + pld.get(i).b.b);
				}
				System.out.println();
				break;
			case "insert":
				printMessage( response);
				Tuple<String,String> insrt = (Tuple<String,String>)payload;
				System.out.println("Inserted <" + insrt.a + "," + insrt.b + ">");
				System.out.println();
				break;
			case "delete":
				printMessage( response);
				String dlt = payload.toString();
				System.out.println("Deleted key "+ dlt );
				System.out.println();
			break;
			case "insertbatch":
				ArrayList<Tuple<String,Tuple<String,Integer>>> newKeys = (ArrayList<Tuple<String,Tuple<String,Integer>>>) payload;
				ArrayList<Tuple<String,Tuple<String,Integer>>> fwdbatch = new ArrayList<Tuple<String,Tuple<String,Integer>>>();
				fwdbatch= insertBatch(newKeys);
				if(fwdbatch.isEmpty()){
					Response departOK = new Response("departOK",null);
					departOK.setDestination(response.getSource());
					departOK.setNode(myId);
					new Thread(new Client("127.0.0.1",next.b,departOK)).start();
				}
				else{
					response.setPayload(fwdbatch);
					new Thread(new Client("127.0.0.1",next.b,response)).start();
				}
				break;
			case "departOK":
				if(port == response.getDestination()){
					departPointers(departIdx);
				}
				else 
					new Thread(new Client("127.0.0.1",next.b,response)).start();
				break;
			case "joinOK":
				if(port == response.getDestination())
						System.out.println("Join Ok");
				else 
					new Thread(new Client("127.0.0.1",next.b,response)).start();
				break;
			case "joinbatch":
				if(response.getDestination() == port){
					ArrayList<Tuple<String,Tuple<String,Integer>>> nKeys = (ArrayList<Tuple<String,Tuple<String,Integer>>>) payload;
					ArrayList<Tuple<String,Tuple<String,Integer>>> fbatch = new ArrayList<Tuple<String,Tuple<String,Integer>>>();
					fbatch= joinBatch(nKeys);
					if(fbatch.isEmpty()){
						Response joinOK = new Response("joinOK",null);
						joinOK.setDestination(response.getSource());
						joinOK.setNode(myId);
						new Thread(new Client("127.0.0.1",next.b,joinOK)).start();
					}
					else{
						response.setPayload(fbatch);
						response.setDestination(next.b);
						new Thread(new Client("127.0.0.1",next.b,response)).start();
					}					
				}
				else{
					new Thread(new Client("127.0.0.1",next.b,response)).start();
				}
				break;
			}
			
			parent.printLatch.release();
			
		}
		
/*********************************** End Handle Requests/Responses ***************************/		
		
		
		
		private synchronized void incWorkers()
		{
			parent.workers++;
		}
		
		private synchronized void decWorkers()
		{
			parent.workers--;
		}
		
		private synchronized void setMaxTime(long current){
			long t = parent.endTime; 
			parent.endTime = (current > t )?current:t;
		}
		 
		 
		public void run(){
			incWorkers();
			
			ObjectInputStream in = null;
			try{
				in = new ObjectInputStream( socket.getInputStream());
				Object messageObj =  null;
				messageObj = in.readObject();
				if( messageObj instanceof XRequest){
					HandleXRequests((XRequest)messageObj);
				}
				else if(messageObj instanceof Request){
					HandleRequests((Request) messageObj);
				}
				else if(messageObj instanceof Response){
					HandleResponse((Response) messageObj);
				}
			} catch (IOException e) {
				decWorkers();
			} catch (ClassNotFoundException e) {
				decWorkers();
			}
			finally{//close resources
				try {
					if(in != null)
						in.close();
					if(socket != null)
						socket.close();
				} catch (IOException e) {
				}
				setMaxTime(System.currentTimeMillis());
				decWorkers();
			}

		}

	}
	
}
