package org.distrib.emulator;
import java.util.Comparator;

import org.distrib.key.Key;
import org.distrib.server.*;
/*
 * Comparator for Eventual Server ids
 */
public class NodeComparator implements Comparator<EventualServer>{
	
	
	public NodeComparator(){
	}
	
	
	@Override
	public int compare(EventualServer s1, EventualServer s2){
		//return compareKeys(s1.myId, s2.myId);
		return Key.compare(s1.myId, s2.myId);
	}
}
