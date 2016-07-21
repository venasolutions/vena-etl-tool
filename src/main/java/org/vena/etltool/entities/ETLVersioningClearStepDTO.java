package org.vena.etltool.entities;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLVersioningClearStepDTO.stepType)
public class ETLVersioningClearStepDTO extends ETLVersioningStepDTO{
	protected final static String stepType = "ETLVersioningClearStep";
	
	public ETLVersioningClearStepDTO() {
		super();
	}
	
	@Override
	public String getName() {
		return "Clearing target values";
	}

}
