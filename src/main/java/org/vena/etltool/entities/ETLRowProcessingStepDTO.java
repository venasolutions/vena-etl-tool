package org.vena.etltool.entities;

public abstract class ETLRowProcessingStepDTO extends ETLStepDTO {
	protected int rowsProcessed;
	private int rowsTotal;
	private int resumeRow;

	public ETLRowProcessingStepDTO() {
		super();
	}

	public int getRowsProcessed() {
		return rowsProcessed;
	}

	public void setRowsProcessed(int rowsProcessed) {
		this.rowsProcessed = rowsProcessed;
	}

	public int getRowsTotal() {
		return rowsTotal;
	}

	public void setRowsTotal(int rowsTotal) {
		this.rowsTotal = rowsTotal;
	}

	public int getResumeRow() {
		return resumeRow;
	}

	public void setResumeRow(int resumeRow) {
		this.resumeRow = resumeRow;
	}

}
