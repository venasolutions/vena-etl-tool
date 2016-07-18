package org.vena.etltool.entities;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLStageToCubeStepDTO.stepType)
public class ETLStageToCubeStepDTO extends ETLImportToCubeStepDTO {

	protected final static String stepType = "ETLStageToCubeStep";

	private String tableName;

	private int skipInvalidRows;

	private int numInvalidRows;

	public ETLStageToCubeStepDTO() {
	}

	public ETLStageToCubeStepDTO(DataType type) {
		this.dataType = type;
		this.rowsProcessed = 0;
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
}
