package org.vena.etltool.entities;

import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.List;

@JsonTypeName(ETLFileToRedshiftStepDTO.stepType)
public class ETLFileToRedshiftStepDTO extends ETLFileImportStepDTO {

	protected final static String stepType = "ETLFileToRedshiftStep";

	private String tableName;

	public ETLFileToRedshiftStepDTO() {
		super();
	}

	public ETLFileToRedshiftStepDTO(ETLFileOldDTO etlFile) {
		super(etlFile);
		this.tableName = etlFile.getTableName();
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	@Override
	public String getName() {
		return "Importing File \""+getFileName()+"\" to Vena Table Destination";
	}
}
