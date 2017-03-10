package org.distrib.emulator;
import java.util.Comparator;

import org.distrib.key.Key;
import org.distrib.server.*;
/*
 * Comparator for linear server ids
 */
public class NodeComparator2 implements Comparator<LinearServer>{
	
	
	public NodeComparator2(){
	}
	
	
	@Override
	public int compare(LinearServer s1, LinearServer s2){
		//return compareKeys(s1.myId, s2.myId);
		return Key.compare(s1.myId, s2.myId);
	}
}
