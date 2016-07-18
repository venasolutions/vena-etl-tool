package org.vena.etltool.entities;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLCubeToStageStepDTO.stepType)
public class ETLCubeToStageStepDTO extends ETLStepDTO{

	protected final static String stepType = "ETLCubeToStageStep";
	public enum QueryType { HQL, MODEL_SLICE }

	private DataType dataType;

	private QueryType queryType;

	private String queryString;

	private String tableName;

	private int rowsExported;

	private int rowsTotal;

	public ETLCubeToStageStepDTO() {
		super();
	}

	public DataType getDataType() {
		return dataType;
	}

	public void setDataType(DataType dataType) {
		this.dataType = dataType;
	}

	public QueryType getQueryType() {
		return queryType;
	}

	public void setQueryType(QueryType queryType) {
		this.queryType = queryType;
	}

	public String getQueryString() {
		return queryString;
	}

	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public int getRowsExported() {
		return rowsExported;
	}

	public void setRowsExported(int rowsExported) {
		this.rowsExported = rowsExported;
	}

	public Integer getRowsTotal() {
		return rowsTotal;
	}

	public void setRowsTotal(Integer rowsTotal) {
		this.rowsTotal = rowsTotal;
	}

	@Override
	public String getName() {
		return "Exporting to SQL Staging Area ("+getDataType()+")";
	}
}
