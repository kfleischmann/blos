package eu.blos.java.sketches;

import java.io.Serializable;

public class HashFunctionDescription implements Serializable{
	public HashFunctionDescription(){
	}
	public HashFunctionDescription(long w, long d){
		this.w = w;
		this.d = d;

	}
	public long w;
	public long d;
}
