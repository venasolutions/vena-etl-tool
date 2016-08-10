package org.vena.etltool.entities;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLCopyIntersectionsStepDTO.stepType)
public class ETLCopyIntersectionsStepDTO extends ETLStepDTO{
	protected final static String stepType = "ETLCopyIntersectionsStep";
	
	private String modelName;
	
	private long rowsProcessed;

	private long rowsTotal;
	
	private Id lastCopiedId;

	private int skipInvalidRows;

	private int numInvalidRows;

	public ETLCopyIntersectionsStepDTO() {
		super();
	}
	
	@Override
	public String getName() {
		return "Copying Intersections";
	}

	public String getModelName() {
		return modelName;
	}

	public void setModelName(String modelName) {
		this.modelName = modelName;
	}
	
	public long getRowsProcessed() {
		return rowsProcessed;
	}

	public void setRowsProcessed(long rowsProcessed) {
		this.rowsProcessed = rowsProcessed;
	}

	public long getRowsTotal() {
		return rowsTotal;
	}

	public void setRowsTotal(long rowsTotal) {
		this.rowsTotal = rowsTotal;
	}

	public Id getLastCopiedId() {
		return lastCopiedId;
	}

	public void setLastCopiedId(Id lastCopiedId) {
		this.lastCopiedId = lastCopiedId;
	}
	
	public int getSkipInvalidRows() {
		return skipInvalidRows;
	}

	public void setSkipInvalidRows(int skipInvalidRows) {
		this.skipInvalidRows = skipInvalidRows;
	}

	public int getNumInvalidRows() {
		return numInvalidRows;
	}

	public void setNumInvalidRows(int numInvalidRows) {
		this.numInvalidRows = numInvalidRows;
	}
}
