package org.distrib.server;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.distrib.client.*;
import org.distrib.emulator.Emulator;


public class Server extends Thread implements Runnable {

	public String myId;
	public boolean coord;
	
	private static final int N = 2048;
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
	
	public void insert(String key, String value){
		System.out.println("inserted key " + key);
		if(mySet.containsKey(key)){
			String oldval = mySet.get(key);
			//update existing value
			mySet.replace(key, oldval, value);
		}
		else{
			mySet.put(key, value);
		}
		System.out.println("Node with Id " + myId + " has the set: " + mySet);
	}
	public void delete(String key){
		System.out.println("Node with Id " + myId  + " to remove key " + key);
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
	
	public void join (String id) throws IOException{
		int port = parent.maxport+1;
		Server node = new Server(id,port,parent);
		System.out.println("Coord with port"+ this.port + " in join for node with port "+ port +" and id " +id );
		int i;
		System.out.println("Size before join= "+parent.nodes.size());
		for(i=0; i<parent.nodes.size(); i++){
			if (Key.compare(parent.nodes.get(i).myId,id)==-1){
				parent.maxport=port;
				node.setNeighbors(parent.nodes.get(i-1).myId, parent.nodes.get(i-1).port, parent.nodes.get(i).myId, parent.nodes.get(i).port);
				parent.nodes.add(i,node);
				System.out.println("Size after join= "+parent.nodes.size());
				return;
				//2 methods for redistribution of keys: 1 for previous and 1 for next
			}
			else if(i==0 && Key.compare(parent.nodes.get(i).myId,id)==1){
				System.out.println("Join to the beginning");
				parent.maxport=port;
				//node.setNeighbors(parent.nodes.get(i-1).myId, parent.nodes.get(i-1).port, parent.nodes.get(i).myId, parent.nodes.get(i).port);
				parent.nodes.add(0,node);
				System.out.println("Size after join= "+parent.nodes.size());
				return;
			}
		}
		parent.nodes.add(node);
	}
	
	public void depart (String id){
		
		int i;
		for(i=0; i<parent.nodes.size(); i++){
			System.out.println("Id1: "+parent.nodes.get(i).myId + " Id2 "+ id);
			if (parent.nodes.get(i).myId.equals(id)){
				System.out.println("Coord with port"+ port + " in depart for node with port "+ parent.nodes.get(i).port);
					//parent.nodes.get(i-1).next=parent.nodes.get(i).next;
					//parent.nodes.get(i).previous=parent.nodes.get(i).previous;
					parent.nodes.remove(i);
					//1 methods for redistribution: next gets all keys
			}
		}
	}
	
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

		@Override
		public void run(){
			BufferedReader in = null;
			try{

				in = new BufferedReader(new InputStreamReader(socket.getInputStream(),"UTF8"));
				String command ;
				while( (command = in.readLine()) != null ){
					//System.out.println("Node at " + port + " received message: " + command);
					String operation = command.substring(0, command.indexOf(','));
					String operands = command.substring(command.indexOf(',') + 1, command.length() ).trim();
					//System.out.println(command);
					//System.out.println(operation);
					//System.out.println(operands);
					int i, comp ;
					String key, value, respondPort, keySHA;
					switch (operation){
					case "insert":
						//System.out.println("Received insert");
						i = operands.indexOf(',');
						key = operands.substring(0, i) ;
						value = operands.substring(i+2,operands.length() );
						//comp = compareKeys(myId,key) ;
						keySHA = Key.generate(key, N);
						comp = Key.compare(myId, keySHA);
						if ( comp == 1 || comp == 0)
						{
							if( Key.compare(previous.a, keySHA) == -1 ){
								insert(key,value);
							}
							else {
								new Thread( new Client("127.0.0.1", next.b, command)).start();
								//send insert command to previous 
							}
						}
						else if (comp == -1) {
							//send insert to next
							new Thread( new Client("127.0.0.1", next.b, command)).start();
						}
						break;
					case "delete":
						i = operands.indexOf(',');
						key = operands.substring(0, i) ;
						keySHA = Key.generate(key, N);
						comp = Key.compare(myId,keySHA) ;
						if ( comp == 1 || comp == 0)
						{
							if( Key.compare(previous.a, keySHA) == -1 ){
								delete(key);
							}
							else {
								new Thread( new Client("127.0.0.1", next.b, command)).start();
								//send delete command to previous 
							}
						}
						else if (comp == -1) {
							new Thread( new Client("127.0.0.1", next.b, command)).start();
							//send delete to next
						}
						break;
					case "query":
						i = operands.indexOf(',');
						key = operands.substring(0, i) ;
						respondPort = operands.substring(i+2,operands.length());
						//comp = compareKeys(myId,key) ;
						keySHA = Key.generate(key, N);
						comp = Key.compare(myId, keySHA);
						if(key.equals("*")){
							incCounter();
							if(parent.counter <= parent.NUM_NODES) {
								ArrayList<Tuple<String,String>> result = new ArrayList<Tuple<String,String>>();
								result = query(key);
								//send message to next
								new Thread( new Client("127.0.0.1", next.b, command)).start();
								//send answer to first client
								System.out.println("Set :" + mySet);
								//new Thread( new Client("127.0.0.1", Integer.parseInt(respondPort), command)).start();
							}
							else 
								resetCounter();
							break;
						}
						if ( comp == 1 || comp == 0)
						{
							if( Key.compare(previous.a, keySHA) == -1 ){
								query(key);
							}
							else {
								//send queery command to previous 
							}
						}
						else if (comp == -1) {
							//send query to next
						}
						break;
					case "show":
						System.out.println("show");
						for(int k=0; k < parent.nodes.size(); k++){
				    		System.out.println("Node: " + parent.nodes.get(k).myId+" with port: "+ parent.nodes.get(k).getLocalPort());
				    	}
						break;
					case "join":
						if (coord ==true){
							System.out.println("Into join");
							i = operands.indexOf(',');
							String id = operands.substring(i+1,operands.length());
							keySHA = Key.generate(id, N);
							join(keySHA);
						}
						break;
					case "depart":
						if (coord ==true){
							System.out.println("Into depart");
							i = operands.indexOf(',');
							String id = operands.substring(i+1,operands.length());
							System.out.println("'"+id+"'");
							keySHA = Key.generate(id, N);
							depart(keySHA);
						}
						break;
					}

				}
			} catch (IOException e) {
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
