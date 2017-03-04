package org.distrib.key;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Key {
		public static String generate(String name, int space) {
			String sha1 = sha1(name);
			int characters = (int) (Math.log(space)/Math.log(2)); //characters of key space (space = 2^m)
			characters = Math.min(characters, sha1.length());
			return sha1.substring(sha1.length() - characters - 1, sha1.length());
		}
		
		
		public static int compare(String k1, String k2){
			float key1 = Float.parseFloat(k1);
			float key2 = Float.parseFloat(k2);
			
			if(key1 > key2) return 1;
			else if(key1 < key2) return -1;
			else return 0;
		
		}
		
		public static String sha1(String s){
			String sha1 = null;
			try {
				MessageDigest md = MessageDigest.getInstance("SHA1");
				byte[] result = md.digest(s.getBytes());
				StringBuffer sb = new StringBuffer();
				for (int i = 0; i < result.length; i++){
					sb.append(Integer.toString(result[i],2).substring(1));
				}
				sha1 = sb.toString();
			} catch (NoSuchAlgorithmException e){
				e.printStackTrace();
			}
			return sha1;
		}
}
