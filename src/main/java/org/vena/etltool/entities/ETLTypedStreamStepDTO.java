package org.vena.etltool.entities;

public abstract class ETLTypedStreamStepDTO extends ETLStreamStepDTO{
	protected DataType type;

	public ETLTypedStreamStepDTO() {
		super();
	}

	public DataType getDataType() {
		return type;
	}

	public void setDataType(DataType type) {
		this.type = type;
	}
}
