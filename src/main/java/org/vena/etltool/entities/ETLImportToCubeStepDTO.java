package org.vena.etltool.entities;

public abstract class ETLImportToCubeStepDTO extends ETLRowProcessingStepDTO{
	protected DataType dataType;
	private boolean createUnmappedMembers = true;

	public ETLImportToCubeStepDTO() {
		super();
	}

	public DataType getDataType() {
		return dataType;
	}

	public void setDataType(DataType type) {
		this.dataType = type;
	}

	public boolean isCreateUnmappedMembers() {
		return createUnmappedMembers;
	}

	public void setCreateUnmappedMembers(boolean createUnmappedMembers) {
		this.createUnmappedMembers = createUnmappedMembers;
	}
}
