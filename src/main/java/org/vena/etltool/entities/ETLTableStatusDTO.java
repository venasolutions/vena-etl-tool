package org.vena.etltool.entities;

public class ETLTableStatusDTO {
	private String tableName;

	private int rowsProcessed;

	private Boolean done = false;

	public ETLTableStatusDTO() {
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public int getRowsProcessed() {
		return rowsProcessed;
	}

	public void setRowsProcessed(int rowsProcessed) {
		this.rowsProcessed = rowsProcessed;
	}

	public Boolean getDone() {
		return done;
	}

	public void setDone(Boolean done) {
		this.done = done;
	}
}
