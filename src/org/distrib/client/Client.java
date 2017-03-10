package org.distrib.client;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import org.distrib.message.Request;
import org.distrib.message.Response;

public class Client extends Thread implements Runnable {
	private Request request= null;
	private Response response = null;
	private Socket socket = null;
	private ObjectOutputStream out = null;
	private int port ;
	private String serverAddress ;
	
	
	public Client(String serverAddress, int port, Request message){
		this.request = message;
		this.port    = port;
		this.serverAddress = serverAddress;
	}
	
	public Client(String serverAddress, int port, Response message){
		this.response = message;
		this.port    = port;
		this.serverAddress = serverAddress;
	}
	
	public void run (){
		//sends Request or Response as objects
		//Request and Response classes are Serializable
		try {
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
