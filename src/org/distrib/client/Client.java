package org.distrib.client;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import org.distrib.message.Message;
import org.distrib.message.Request;
import org.distrib.message.Response;

public class Client extends Thread implements Runnable {
	private boolean startedStarQuery = false;
	private Request request= null;
	private Response response = null;
	private Socket socket = null;
	private ObjectOutputStream out = null;
	private int port ;
	private String serverAddress ;
	
	
	public Client(String serverAddress, int port, Request message){
		//super(serverAddress, port);
		this.request = message;
		this.port    = port;
		this.serverAddress = serverAddress;
	}
	
	public Client(String serverAddress, int port, Response message){
		//super(serverAddress, port);
		this.response = message;
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
			// we connect the client-socket with server's port so as to handle the request 
			socket = new Socket();
			socket.connect(new InetSocketAddress(serverAddress,port));
			out = new ObjectOutputStream(socket.getOutputStream());
			out.flush();
			if(request != null)
				out.writeObject(request);
			else if(response != null)
				out.writeObject(response);
			
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
