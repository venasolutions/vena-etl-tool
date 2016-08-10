package org.vena.etltool.entities;

import java.io.Serializable;

public class Id implements Serializable, Comparable<Id> {

	private static final long serialVersionUID = 2169865468343288369L;

	public static Id valueOf(String longStr) {
		return new Id(Long.parseLong(longStr));
	}

	long idValue;

	public Id() {

	}

	public Id(long idValue) {
		super();
		this.idValue = idValue;
	}

	public long longValue() {
		return idValue;
	}

	public void setIdValue(long idValue) {
		this.idValue = idValue;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (idValue ^ (idValue >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Id other = (Id) obj;
		if (idValue != other.idValue)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return Long.toString(idValue);
	}

	@Override
	public int compareTo(Id o) {
		long diff = this.idValue - o.idValue;
		if (diff > 0) return 1;
		if (diff < 0) return -1;
		return 0;
	}

}