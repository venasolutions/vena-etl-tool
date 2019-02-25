package org.vena.etltool.entities;

import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.ArrayList;
import java.util.List;

@JsonTypeName(ETLFileToVenaTableStepDTO.stepType)
public class ETLFileToVenaTableStepDTO extends ETLFileImportStepDTO {

	protected final static String stepType = "ETLFileToVenaTableStep";

	private String tableName;

	private List<String> clearSlicesColumns = new ArrayList<>();

	public ETLFileToVenaTableStepDTO() {
		super();
	}

	public ETLFileToVenaTableStepDTO(ETLFileOldDTO etlFile) {
		super(etlFile);
		this.tableName = etlFile.getTableName();
		this.clearSlicesColumns = etlFile.getClearSlicesColumns();
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

	public void setClearSlicesColumns(List<String> clearSlicesColumns) {
		this.clearSlicesColumns = clearSlicesColumns;
	}

	public List<String> getClearSlicesColumns() {
		return clearSlicesColumns;
	}
}
