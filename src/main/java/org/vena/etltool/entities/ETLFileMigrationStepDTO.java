package org.vena.etltool.entities;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLFileMigrationStepDTO.stepType)

public class ETLFileMigrationStepDTO extends ETLStepDTO {
	
	protected final static String stepType = "ETLFileMigrationStep";
	
	private boolean copyingSelected;
	
	private String cloneModelName;
	
	private List<Id> fileList;
	
	private List<String> fileNames;

	public ETLFileMigrationStepDTO() {
		super();
	}
	
	@Override
	public String getName() {
		return "Migrating Data Files";
	}

	public String getCloneModelName() {
		return cloneModelName;
	}

	public void setCloneModelName(String cloneModelName) {
		this.cloneModelName = cloneModelName;
	}

	public List<Id> getFileList() {
		return fileList;
	}

	public void setFileList(List<Id> fileList) {
		this.fileList = fileList;
	}

	public boolean isCopyingSelected() {
		return copyingSelected;
	}

	public void setCopyingSelected(boolean copyingSelected) {
		this.copyingSelected = copyingSelected;
	}

	public List<String> getFileNames() {
		return fileNames;
	}

	public void setFileNames(List<String> fileNames) {
		this.fileNames = fileNames;
	}
}
