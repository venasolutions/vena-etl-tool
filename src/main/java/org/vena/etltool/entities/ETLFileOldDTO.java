package org.vena.etltool.entities;

import java.util.List;

import org.vena.etltool.entities.ETLFileImportStepDTO.FileFormat;
import org.vena.etltool.entities.ETLStepDTO.DataType;

public class ETLFileOldDTO {
	private static String supportedFileTypes;

	static {
		StringBuilder buf = new StringBuilder();

		for(DataType value : DataType.values()) {
			buf.append(value.toString());
			buf.append(", ");
		}
		
		//Return as String and strip trailing ", ".
		supportedFileTypes = buf.substring(0, buf.length()-2);
	}
	
	public static final String SUPPORTED_FILETYPES_LIST = supportedFileTypes;
	
	private String mimePart;

	private FileFormat fileFormat;

	private DataType fileType;

	private String filename;
	
	private String serverFilename;
	
	private int linesProcessed;
	
	private Boolean done = false;

	private List<String> columnNames;
	
	private String tableName;

	private boolean bulkInsert;

	private List<String> clearSlicesExpressions = null;
	
	public ETLFileOldDTO() {
		super();
		this.linesProcessed=0;
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public String getMimePart() {
		return mimePart;
	}

	public void setMimePart(String mimePart) {
		this.mimePart = mimePart;
	}

	public FileFormat getFileFormat() {
		return fileFormat;
	}

	public void setFileFormat(FileFormat fileFormat) {
		this.fileFormat = fileFormat;
	}

	public DataType getFileType() {
		return fileType;
	}

	public void setFileType(DataType fileType) {
		this.fileType = fileType;
	}

	public String getServerFilename() {
		return serverFilename;
	}

	public void setServerFilename(String serverFilename) {
		this.serverFilename = serverFilename;
	}

	public int getLinesProcessed() {
		return linesProcessed;
	}

	public void setLinesProcessed(int linesProcessed) {
		this.linesProcessed = linesProcessed;
	}
	
	public Boolean getDone() {
		return done;
	}

	public void setDone(Boolean done) {
		this.done = done;
	}

	public final int incrementLinesProcessed() {
		return linesProcessed = linesProcessed+1;
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
	
	public void setClearSlicesExpressions(List<String> clearSlicesExpressions) {
		this.clearSlicesExpressions = clearSlicesExpressions;
	}
	
	public List<String> getClearSlicesExpressions() {
		return this.clearSlicesExpressions;
	}

}
