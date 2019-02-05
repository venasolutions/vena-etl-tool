package org.vena.etltool.entities;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLFileToVenaTableStepDTO.stepType)
public class ETLFileToVenaTableStepDTO extends ETLFileImportStepDTO {

	protected final static String stepType = "ETLFileToVenaTableStep";

	private String tableName;

	public ETLFileToVenaTableStepDTO() {
		super();
	}

	public ETLFileToVenaTableStepDTO(ETLFileOldDTO etlFile) {
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
