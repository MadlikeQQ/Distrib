package org.distrib.client;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class Client extends Thread implements Runnable {
	private boolean startedStarQuery = false;
	private String message = null;
	private Socket socket = null;
	private OutputStream out = null;
	private int port ;
	private String serverAddress ;
	
	
	public Client(String serverAddress, int port, String message){
		//super(serverAddress, port);
		this.message = message;
		this.port    = port;
		this.serverAddress = serverAddress;
	}
	
	public void sendMessage(String message) throws IOException{
		socket.getOutputStream().flush();
		socket.getOutputStream().write(message.getBytes());
		socket.close();
		//if message = query * set startedStar = true;
	}
	
	public void run (){
		try {
			socket = new Socket();
			socket.connect(new InetSocketAddress(serverAddress,port));
			out = socket.getOutputStream();
			out.flush();
			out.write(message.getBytes());
		} 
		catch (IOException e) {
		}
		finally {
			try {
				if(out!=null)
					out.close();
				if(socket!=null)
					socket.close();	
			} 
			catch (IOException e) {
			}
		}
	}
}
