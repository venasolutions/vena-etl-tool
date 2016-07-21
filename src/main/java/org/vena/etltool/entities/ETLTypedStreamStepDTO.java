package org.vena.etltool.entities;

import org.vena.api.customer.etl.transform.RowSource;

public abstract class ETLTypedStreamStepDTO extends ETLStreamStepDTO{
	protected DataType type;

	public ETLTypedStreamStepDTO() {
		super();
	}

	public ETLTypedStreamStepDTO(RowSource source, MockMode mockMode) {
		super(source, mockMode);
		type = DataType.valueOf(source.getDataType());
	}

	public DataType getDataType() {
		return type;
	}

	public void setDataType(DataType type) {
		this.type = type;
	}
}
