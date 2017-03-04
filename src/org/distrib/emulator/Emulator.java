package org.distrib.emulator;
import java.io.BufferedReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import org.distrib.client.Client;
import org.distrib.key.Key;
import org.distrib.server.Server;


public class Emulator {
	
	
	public static final int NUM_NODES = 10;
	private Server[] nodes;
	private Client[] clients;
	public volatile int counter = 0;
	private static final int N = 1024;
	
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
		
		
		
		nodes = new Server[NUM_NODES];
		clients = new Client[NUM_NODES];
		int i;
		for (i = 0 ; i <NUM_NODES; i++ ){
			String id = Integer.toString(i);
			//md.update(id.getBytes());
			//nodes[i] = new Server(md.digest().toString(), port,this);
			nodes[i] = new Server(Key.generate(id, N),port,this);
			port++;
		}
		
		Arrays.sort(nodes,new NodeComparator());
		
		for (i = 0 ; i < NUM_NODES ; i++){
			
			if(i==0){
				nodes[i].setNeighbors(nodes[NUM_NODES-1].myId, nodes[NUM_NODES-1].getLocalPort(), nodes[i+1].myId, nodes[i+1].getLocalPort());
			}
			else if (i==NUM_NODES - 1){
				nodes[i].setNeighbors(nodes[i-1].myId, nodes[i-1].getLocalPort(), nodes[0].myId, nodes[0].getLocalPort());
			}
			else{
				nodes[i].setNeighbors(nodes[i-1].myId, nodes[i-1].getLocalPort(), nodes[i+1].myId, nodes[i+1].getLocalPort());
			}
			
			new Thread(nodes[i]).start();
		}
		
		
		int j = 0;
		Charset charset = Charset.forName("US-ASCII");
		Path file =  Paths.get("insert.txt");
		BufferedReader reader = Files.newBufferedReader(file,charset);
		String line;
		while( (line=reader.readLine()) != null   && j < 21){
			i = (int) (Math.random() * (NUM_NODES - 1)) + 4444;
			//System.out.println("Emulator chose port " + i);
			/*
			 * Serial
			 */
			///*
			Socket s = new Socket("127.0.0.1", i);
			String m = "insert, "+line;
			s.getOutputStream().flush();
			s.getOutputStream().write(m.getBytes());
			s.close();
			//*/
			/*
			 * Async
			 */
			
			//new Thread( new Client("127.0.0.1", i,"insert, "+line)).start();
			
			//j++;
		}

		new Thread( new Client("127.0.0.1", i,"query, *, "+i)).start();
		
		//System.exit(0); to see if shutdown hook works
		
	}
	
	
	public static void main(String[] args) throws IOException
	{
		Emulator em = new Emulator();
		em.run();
		
	}
}
