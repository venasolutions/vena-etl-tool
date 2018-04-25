package org.vena.etltool.entities;

import java.util.Date;
import java.util.List;

import org.vena.etltool.entities.ETLFileImportStepDTO.FileFormat;

public class QueryDTO {

	public enum Destination { ToCSV, ToStaging }

	private Id modelId;

	private String queryString;
	
	private Date timestamp;
	
	private Destination destination;
	
	private String tableName;
	
	private List<String> columnNames;
	
	private boolean previewMode = false;

	private boolean showHeaders = true;
	
	private boolean exportMemberIds = false;

	private List<String> filterColumnNames;
	
	private FileFormat format;

	public QueryDTO()
	{
	}

	public boolean isTableNamePresent() {
		if (destination == Destination.ToStaging && (tableName == null || tableName.isEmpty())) return false;
		return true;
	}

	public Id getModelId() {
		return modelId;
	}

	public void setModelId(Id modelId) {
		this.modelId = modelId;
	}

	public String getQueryString() {
		return queryString;
	}

	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}

	public Destination getDestination() {
		return destination;
	}

	public void setDestination(Destination destination) {
		this.destination = destination;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<String> getColumnNames() {
		return columnNames;
	}

	public void setColumnNames(List<String> columnNames) {
		this.columnNames = columnNames;
	}

	public boolean isPreviewMode() {
		return previewMode;
	}
	
	public void setPreviewMode(boolean previewMode) {
		this.previewMode = previewMode;
	}
	
	public boolean isShowHeaders() {
		return showHeaders;
	}

	public void setShowHeaders(boolean showHeaders) {
		this.showHeaders = showHeaders;
	}

	public List<String> getFilterColumnNames() {
		return filterColumnNames;
	}

	public void setFilterColumnNames(List<String> filterColumnNames) {
		this.filterColumnNames = filterColumnNames;
	}

	public boolean isExportMemberIds() {
		return exportMemberIds;
	}

	public void setExportMemberIds(boolean exportMemberIds) {
		this.exportMemberIds = exportMemberIds;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	public FileFormat getFormat() {
		return format;
	}

	public void setFormat(FileFormat format) {
		this.format = format;
	}
}
