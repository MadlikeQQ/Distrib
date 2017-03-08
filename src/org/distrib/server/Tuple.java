package org.distrib.server;

import java.io.Serializable;

public class Tuple<A,B> implements Serializable {
	 public final A a; 
	    public B b; 
	    public Tuple(A a, B b) { 
	        this.a = a; 
	        this.b = b;
	    }
}
