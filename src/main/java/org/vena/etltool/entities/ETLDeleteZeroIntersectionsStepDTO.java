package org.vena.etltool.entities;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLDeleteZeroIntersectionsStepDTO.stepType)
public class ETLDeleteZeroIntersectionsStepDTO extends ETLStepDTO{
protected final static String stepType = "ETLDeleteZeroIntersectionsStep";
	
	private DataType dataType;

	public ETLDeleteZeroIntersectionsStepDTO() {
		super();
	}
	
	@Override
	public String getName() {
		return "Deleting Zero Intersections";
	}

	public DataType getDataType() {
		return dataType;
	}

	public void setDataType(DataType dataType) {
		this.dataType = dataType;
	}
}
