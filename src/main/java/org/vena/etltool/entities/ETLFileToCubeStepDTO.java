package org.vena.etltool.entities;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLFileToCubeStepDTO.stepType)

public class ETLFileToCubeStepDTO extends ETLFileImportStepDTO{
	protected final static String stepType = "ETLFileToCubeStep";

	private boolean createUnmappedMembers = true;

	public ETLFileToCubeStepDTO() {
	}

	public ETLFileToCubeStepDTO(ETLFileOldDTO etlFile) {
		super(etlFile);
	}

	@Override
	public String getName() {
		return "Importing File \""+getFileName()+"\" ("+getDataType()+")";
	}

	public boolean isCreateUnmappedMembers() {
		return createUnmappedMembers;
	}

	public void setCreateUnmappedMembers(boolean createUnmappedMembers) {
		this.createUnmappedMembers = createUnmappedMembers;
	}
}
