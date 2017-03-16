package org.distrib.emulator;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import org.distrib.client.Client;
import org.distrib.key.Key;
import org.distrib.message.Request;
import org.distrib.server.LinearServer;

import com.sun.corba.se.impl.orbutil.concurrent.Mutex;

/*
 * 
 * Spawn nodes with linearization and Chain replication
 */
public class Emulator2 {
	
	public int NUM_NODES = 10;
	public ArrayList<LinearServer> nodes;
	public volatile int counter = 0;
	public final int N=1048576;
	public int maxport=4440;
	public int coord_port;
	private int replicas = 5;
	public volatile long endTime ;
	public volatile int barrier = NUM_NODES;
	public volatile int workers = 0;
	public Mutex printLatch = new Mutex();
	public CountDownLatch startSignal = new CountDownLatch(NUM_NODES); 
	public long startT = System.currentTimeMillis();
	public Emulator2 ( ) {}
	
	public void run() throws IOException{
		int port = 4440;
		
		nodes = new ArrayList<LinearServer>();
		int i;
		for (i = 0 ; i <NUM_NODES; i++ ){
			String id = Integer.toString(i);
			LinearServer node = new LinearServer(Key.sha1(id),port,this, replicas);
			nodes.add(node);
			maxport=port;
			port++;
			
		}
		
		Collections.sort(nodes,new NodeComparator2());
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
		Path file = Paths.get("insert.txt"); ;
		BufferedReader reader = Files.newBufferedReader(file,charset);;
		String line;
		
		while( (line=reader.readLine()) != null )   {
			i = (int) (Math.random() * (NUM_NODES - 1));
			
			Request r = new Request("insert, " +line);
			r.setSource(nodes.get(i).getLocalPort());
			new Thread(new Client("127.0.0.1",nodes.get(i).getLocalPort(),r)).start();
			j++;
		}
		for(int k=0; k < nodes.size(); k++){
    		System.out.println("Node: " + ((nodes.get(k).myId)) +" with port: "+ nodes.get(k).getLocalPort());
    	}
		


	//	startT = System.currentTimeMillis();
		file =  Paths.get("query.txt");
		startT = System.currentTimeMillis();
/*		file =  Paths.get("query.txt");

		reader = Files.newBufferedReader(file,charset);
		j = 1;
		while( (line=reader.readLine()) != null )   {
			i = (int) (Math.random() * (NUM_NODES - 1));
			Request r = new Request("query, " + line);
			r.setSerialVersionID((long) j);//for next request
			r.setSource(nodes.get(i).getLocalPort());
			new Thread(new Client("127.0.0.1",nodes.get(i).getLocalPort(),r)).start();
			j++;
		}
		
	/*	float elapsed;
		while(true){
			
			elapsed = (float) (endTime - startT);
			System.out.println("Total Running time " + elapsed + " (time/op): " + (float)elapsed / ((j-1)*1000));
			//System.out.println((float) (endTime- startT) / ((j-1) * 1000 ));
		}*/
		/*
		 * Uncomment below for interactive input to emulator
		 */
		

	    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String input;
	    while((input = br.readLine()) != null){
	    	Request tmp = new Request(input);
	    	if(tmp.getOperation().equals("join") || tmp.getOperation().equals("depart")){
	    		new Thread( new Client("127.0.0.1", coord_port,tmp)).start();
	    	}
	    	else{
	    		i = (int) (Math.random() * (NUM_NODES - 1));
	    		tmp.setSource(nodes.get(i).getLocalPort());
	    		tmp.setSerialVersionID(j);
	    		new Thread( new Client("127.0.0.1", nodes.get(i).getLocalPort(),tmp)).start();
	    	}		
	    	j++;
	    }
	}

	public static void main(String[] args) throws IOException
	{
		Emulator2 em = new Emulator2();
		em.run();
		
	}
}
