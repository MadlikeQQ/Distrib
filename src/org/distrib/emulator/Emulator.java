package org.distrib.emulator;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;

import org.distrib.client.Client;
import org.distrib.key.Key;
import org.distrib.message.Request;
import org.distrib.server.Server;


public class Emulator {
	
	
	public int NUM_NODES = 10;
	public ArrayList<Server> nodes;
	private Client[] clients;
	public volatile int counter = 0;
	public final int N=1048576;
	public int maxport=4444;
	public int coord_port;
	
	CountDownLatch startSignal = new CountDownLatch(NUM_NODES); 
	
	public Emulator ( ) {}
	
	public void run() throws IOException{
		int port = 4444;
		MessageDigest md = null; 
		try {
			md = MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		nodes = new ArrayList<Server>();
		clients = new Client[NUM_NODES];
		int i;
		for (i = 0 ; i <NUM_NODES; i++ ){
			String id = Integer.toString(i);
			//md.update(id.getBytes());
			//nodes[i] = new Server(md.digest().toString(), port,this);
			Server node = new Server(Key.sha1(id),port,this);
			nodes.add(node);
			port++;
			maxport=port;
		}
		
		Collections.sort(nodes,new NodeComparator());
		
		for (i = 0 ; i < NUM_NODES ; i++){
			//Number of current nodes in list
			
			if(i==0){
				nodes.get(i).setNeighbors(nodes.get(NUM_NODES-1).myId, nodes.get(NUM_NODES-1).getLocalPort(), nodes.get(i+1).myId, nodes.get(i+1).getLocalPort());
			}
			else if (i==NUM_NODES - 1){
				nodes.get(i).setNeighbors(nodes.get(i-1).myId, nodes.get(i-1).getLocalPort(), nodes.get(0).myId, nodes.get(0).getLocalPort());
			}
			else{
				nodes.get(i).setNeighbors(nodes.get(i-1).myId, nodes.get(i-1).getLocalPort(), nodes.get(i+1).myId, nodes.get(i+1).getLocalPort());
			}
			
			new Thread(nodes.get(i)).start();
		}
		
		nodes.get(0).coord=true;
		coord_port=nodes.get(0).getLocalPort();
		
		int j = 1;
		Charset charset = Charset.forName("US-ASCII");
		Path file =  Paths.get("test.txt");
		BufferedReader reader = Files.newBufferedReader(file,charset);
		String line;
		while( (line=reader.readLine()) != null && j <=10)   {
			i = (int) (Math.random() * (NUM_NODES - 1)) + 4444;
			//System.out.println("Emulator chose port " + i);
			/*
			 * Serial
			 */
			///*
			//Socket s = new Socket("127.0.0.1", i);
			
			Request r = new Request(line);
			r.setSource(i);
			new Thread(new Client("127.0.0.1",i,r)).start();
			
			//s.getOutputStream().flush();
			//s.getOutputStream().write(m.getBytes());
			//s.close();
			//*/
			/*
			 * Async
			 */
			
			//new Thread( new Client("127.0.0.1", i,"insert, "+line)).start();
			
			j++;
		}
		//Request p = new Request("query, *");
		//p.setSource(i);
		//new Thread( new Client("127.0.0.1", i,p)).start();
		for(int k=0; k < nodes.size(); k++){
    		System.out.println("Node: " + Key.toHex((nodes.get(k).myId)) +" with port: "+ nodes.get(k).getLocalPort());
    	}
		//System.exit(0); to see if shutdown hook works
	    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String input;
	    while((input = br.readLine()) != null){
	    	//Socket sct = new Socket("127.0.0.1",coord_port);
	    	//sct.getOutputStream().flush();
	    	//sct.getOutputStream().write(input.getBytes());
	    	//sct.close();
	    	Request tmp = new Request(input);
	    	
	    	if(tmp.getOperation().equals("join") || tmp.getOperation().equals("depart")){
	    		new Thread( new Client("127.0.0.1", coord_port,tmp)).start();
	    	}
	    	else{
	    		i = ThreadLocalRandom.current().nextInt(4444, maxport  + 1);
	    		tmp.setSource(i);
	    		new Thread( new Client("127.0.0.1", i,tmp)).start();
	    	}
	    		
	    }
		
	}

	public static void main(String[] args) throws IOException
	{
		Emulator em = new Emulator();
		em.run();
		
	}
}
