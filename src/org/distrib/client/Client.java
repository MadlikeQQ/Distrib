package org.distrib.client;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class Client extends Socket {
	public Client(String serverAddress, int port) throws UnknownHostException, IOException{
		super(serverAddress, port);
	}
	
	
	public void sendMessage(String message) throws IOException{
		this.getOutputStream().flush();
		this.getOutputStream().write(message.getBytes());
	}
	
	
	public static synchronized void main(String[] args) throws UnknownHostException, IOException{
		Client c = new Client("127.0.0.1", 4444);
		c.sendMessage("insert, Cafo, 1\ninsert, Cafo, 15\n");
		c.close();
		//System.exit(0);
	}
}
