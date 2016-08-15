package org.vena.etltool.entities;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLCopyLIDsStepDTO.stepType)

public class ETLCopyLIDsStepDTO extends ETLStepDTO {
	
	protected final static String stepType = "ETLCopyLIDsStep";
	
	private String modelName;
	
	private long rowsProcessed;

	private long rowsTotal;
	
	private Id lastCopiedId;

	private int skipInvalidRows;

	private int numInvalidRows;

	public ETLCopyLIDsStepDTO() {
		super();
	}
	
	@Override
	public String getName() {
		return "Copying Line Item Details";
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
