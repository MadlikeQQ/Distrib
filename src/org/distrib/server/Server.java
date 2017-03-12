package org.distrib.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

import org.distrib.emulator.Emulator;
import org.distrib.message.XRequest;

public class Server implements Runnable{
	
	public String myId = "";
	public boolean coord = false;
	
	public int N = 1048576;
	static final int MIN_POOL_SIZE = 1;
	static final int MAX_POOL_SIZE = 200;
	static final int DEFAULT_ALIVE_TIME = 60000;
	
	HashMap<String,Tuple<String,Integer>> mySet = null;
	Tuple<String,Integer> next =null;
	Tuple<String,Integer> previous = null;
	int port = 0;
	public Emulator parent = null;
	ThreadPoolExecutor workerThreadPool = null;
	ThreadPoolExecutor clientThreadPool = null;
	boolean hasToRun = true;
	boolean running = false;
	public ServerSocket serverSocket = null;
	CountDownLatch startSignal = new CountDownLatch(1) ;
	int K=3 ;
	int departIdx = -1;
	
	public void shutdown(){}

	public int getLocalPort() {
		// TODO Auto-generated method stub
		return 0;
	}

	public void queueShutdown() {
		// TODO Auto-generated method stub
		
	}

	public void setNeighbors(String myId2, int localPort, String myId3, int localPort2) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}
}