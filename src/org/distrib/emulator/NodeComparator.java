package org.distrib.emulator;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;

import org.distrib.key.Key;
import org.distrib.server.*;

public class NodeComparator implements Comparator<Server>{
	
	private MessageDigest md = null;
	
	public NodeComparator(){
		/*try {
			md = MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
	}
	
	
	@Override
	public int compare(Server s1, Server s2){
		//return compareKeys(s1.myId, s2.myId);
		return Key.compare(s1.myId, s2.myId);
	}
	
	
	private int compareKeys(String nodeID, String key){
		byte[] sha1 = nodeID.getBytes(); 

		md.update(key.getBytes());
		byte[] sha2 = md.digest();

		for (int i = 0; i < sha1.length; i++ ){
			if(sha1[i] < sha2[i]) return -1;
			else if (sha1[i] > sha2[i]) return 1;
		}
		return 0;
	}
}
