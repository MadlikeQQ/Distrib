package org.distrib.key;

import java.nio.charset.StandardCharsets;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class Key {
		public static String generate(String name, int space) {
			String sha1 = sha1(name);
			int characters = (int) (Math.log(space)/Math.log(2)); //characters of key space (space = 2^m)
			characters = Math.min(characters, sha1.length());
			return sha1.substring(sha1.length() - characters - 1, sha1.length());
		}
		
		
		public static int compare(String key1, String key2){
			//float key1 = Float.parseFloat(k1);
			//float key2 = Float.parseFloat(k2);
			char[] k1 = key1.toCharArray();
			char[] k2 = key2.toCharArray();
			
			for (int i=0; i< k1.length; i++){
				if(k1[i] > k2[i]) return 1;
				else if(k1[i] < k2[i]) return -1;
			}
			return 0;
		}
		
		public static boolean between(String key, String from, String to ){
			if(compare(from, to) ==  1){//if from > to
				return (compare(key, from) == 1) || ( compare(key, to) == -1 || compare(key, to) == 0);
			}
			else if (compare(from, to ) == -1) {
				return (compare(key, from ) == 1) && ( compare(key, to) == -1 || compare(key, to) == 0);
			}
			else return true;
		}
		
		public static String sha1(String s){
			String sha1 = null;
			try {
				MessageDigest md = MessageDigest.getInstance("SHA1");
				byte[] result = new byte[40];
				Arrays.fill(result,(byte)0);
				md.update(s.getBytes());
				//System.out.printf("Digest length %d" ,md.getDigestLength());
				md.digest(result, 40 - md.getDigestLength()  ,md.getDigestLength());
				
				sha1 = new String(result, StandardCharsets.ISO_8859_1);
				//System.out.println("Sha length = " + sha1.getBytes().length);
			} catch (NoSuchAlgorithmException e){
				e.printStackTrace();
			} catch (DigestException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return sha1;
		}
		
		public static String toHex(String s){
			return convertByteArrayToHexString(s.getBytes());
		}
		
		private static String convertByteArrayToHexString(byte[] arrayBytes) {
		    StringBuffer stringBuffer = new StringBuffer();
		    for (int i = 0; i < arrayBytes.length; i++) {
		        stringBuffer.append(Integer.toString((arrayBytes[i] & 0xff) + 0x100, 32)
		                .substring(1));
		    }
		    return stringBuffer.toString();
		}
}
