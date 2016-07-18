package org.vena.etltool.entities;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLDeleteValuesStepDTO.stepType)
public class ETLDeleteValuesStepDTO extends AbstractETLDeleteStepDTO{
	protected final static String stepType = "ETLDeleteValuesStep";

	public ETLDeleteValuesStepDTO() {
		super();
	}
	
	@Override
	public String getName() {
		return "Deleting Intersection Values";
	}
	
}
