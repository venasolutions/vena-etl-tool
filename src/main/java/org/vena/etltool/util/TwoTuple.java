package org.vena.etltool.util;


public class TwoTuple<T1,T2> {
	
	T1 o1;
	
	T2 o2;

	public TwoTuple(T1 o1, T2 o2) {
		super();
		this.o1 = o1;
		this.o2 = o2;
	}

	public T1 getO1() {
		return o1;
	}

	public void setO1(T1 o1) {
		this.o1 = o1;
	}

	public T2 getO2() {
		return o2;
	}

	public void setO2(T2 o2) {
		this.o2 = o2;
	}
	
	

}
