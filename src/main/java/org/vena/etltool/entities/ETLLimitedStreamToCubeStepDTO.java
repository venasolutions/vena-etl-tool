package org.vena.etltool.entities;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLLimitedStreamToCubeStepDTO.stepType)
public class ETLLimitedStreamToCubeStepDTO extends ETLStreamToCubeStepDTO{
	private Integer firstRow;
	private Integer limit;

	protected final static String stepType = "ETLLimitedStreamToCubeStep";


	public ETLLimitedStreamToCubeStepDTO() { // For mongo
		super();
	}

	public Integer getFirstRow() {
		return firstRow;
	}

	public void setFirstRow(Integer firstRow) {
		this.firstRow = firstRow;
	}

	public Integer getRowCountLimit() {
		return limit;
	}

	public void setLimit(Integer limit) {
		this.limit = limit;
	}
}
