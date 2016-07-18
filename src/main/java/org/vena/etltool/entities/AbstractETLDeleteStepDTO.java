package org.vena.etltool.entities;

public abstract class AbstractETLDeleteStepDTO extends ETLStepDTO {

	protected String expression;
	
	protected DataType dataType;
	
	protected long numDeleted;
	
	protected long numLidsDeleted;
	
	protected long numIntersectionsScanned;

	public String getExpression() {
		return expression;
	}

	public void setExpression(String queryString) {
		this.expression = queryString;
	}
	
	public DataType getDataType() {
		return dataType;
	}

	public void setDataType(DataType dataType) {
		this.dataType = dataType;
	}

	public long getNumDeleted() {
		return numDeleted;
	}

	public void setNumDeleted(long numDeleted) {
		this.numDeleted = numDeleted;
	}
	
	public long getNumLidsDeleted() {
		return numLidsDeleted;
	}

	public void setNumLidsDeleted(long numLidsDeleted) {
		this.numLidsDeleted = numLidsDeleted;
	}
	
	public long getNumIntersectionsScanned() {
		return numIntersectionsScanned;
	}

	public void setNumIntersectionsScanned(long numIntersectionsScanned) {
		this.numIntersectionsScanned = numIntersectionsScanned;
	}
}
