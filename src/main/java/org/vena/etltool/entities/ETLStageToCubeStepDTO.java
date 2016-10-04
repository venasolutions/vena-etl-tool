package org.vena.etltool.entities;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLStageToCubeStepDTO.stepType)
public class ETLStageToCubeStepDTO extends ETLImportToCubeStepDTO {

	protected final static String stepType = "ETLStageToCubeStep";

	private String tableName;

	private int skipInvalidRows;

	private int numInvalidRows;
	
	private List<String> clearSlicesExpressions = null;
	
	private long seqNum;
	
	private long numDeleted;
	
	private long numLidsDeleted;

	public ETLStageToCubeStepDTO() {
	}

	public ETLStageToCubeStepDTO(DataType type) {
		this.dataType = type;
		this.rowsProcessed = 0;
	}
	
	public ETLStageToCubeStepDTO(DataType type, List<String> clearSlicesExpressions) {
		this.dataType = type;
		this.clearSlicesExpressions = clearSlicesExpressions;
	}

	public ETLStageToCubeStepDTO(ETLTableStatusDTO table) {
		this.tableName = table.getTableName();

		switch (tableName) {
		case "out_attributes":
			this.dataType = DataType.attributes;
			break;
		case "out_hierarchies":
			this.dataType = DataType.hierarchy;
			break;
		case "out_lids":
			this.dataType = DataType.lids;
			break;
		case "out_values":
			this.dataType = DataType.intersections;
			break;
		}

		this.rowsProcessed = table.getRowsProcessed();
		if (table.getDone()) {
			this.status = Status.COMPLETED;
			this.percentDone = 100;
		}
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	@Override
	public String getName() {
		return "Importing from SQL Staging Area ("+getDataType()+")";
	}

	public int getSkipInvalidRows() {
		return skipInvalidRows;
	}

	public void setSkipInvalidRows(int maxErrors) {
		this.skipInvalidRows = maxErrors;
	}

	public int getNumInvalidRows() {
		return numInvalidRows;
	}

	public void setNumInvalidRows(int numErrors) {
		this.numInvalidRows = numErrors;
	}
	
	public void setClearSlicesExpressions(List<String> clearSlicesExpressions) {
		this.clearSlicesExpressions = clearSlicesExpressions;
	}
	
	public List<String> getClearSlicesExpressions() {
		return clearSlicesExpressions;
	}
	
	public long getSeqNum() {
		return seqNum;
	}
	
	public void setSeqNum(long seqNum) {
		this.seqNum = seqNum;
	}
	
	public void setNumDeleted(long numDeleted) {
		this.numDeleted = numDeleted;
	}

	public long getNumDeleted() {
		return numDeleted;
	}

	public void setNumLidsDeleted(long numLidsDeleted) {
		this.numLidsDeleted = numLidsDeleted;
	}

	public long getNumLidsDeleted() {
		return numLidsDeleted;
	}
}
