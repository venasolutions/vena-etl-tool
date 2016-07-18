package org.vena.etltool.entities;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLFileToStageStepDTO.stepType)
public class ETLFileToStageStepDTO extends ETLFileImportStepDTO {

	protected final static String stepType = "ETLFileToStageStep";

	private List<String> columnNames;

	private String tableName;

	private boolean bulkInsert;

	public ETLFileToStageStepDTO() {
		super();
	}

	public ETLFileToStageStepDTO(ETLFileOldDTO etlFile) {
		super(etlFile);
		this.bulkInsert = etlFile.isBulkInsert();
		this.columnNames = etlFile.getColumnNames();
		this.tableName = etlFile.getTableName();
	}

	public List<String> getColumnNames() {
		return columnNames;
	}

	public void setColumnNames(List<String> columnNames) {
		this.columnNames = columnNames;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public boolean isBulkInsert() {
		return bulkInsert;
	}

	public void setBulkInsert(boolean bulkInsert) {
		this.bulkInsert = bulkInsert;
	}

	@Override
	public String getName() {
		return "Importing File \""+getFileName()+"\" to SQL Staging Area";
	}
}
