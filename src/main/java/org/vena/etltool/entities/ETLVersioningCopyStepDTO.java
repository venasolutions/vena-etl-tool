package org.vena.etltool.entities;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLVersioningCopyStepDTO.stepType)
public class ETLVersioningCopyStepDTO extends ETLVersioningStepDTO{
	
	protected final static String stepType = "ETLVersioningCopyStep";
	
	public ETLVersioningCopyStepDTO() {
		super();
	}
	
	@Override
	public String getName() {
		return "Copying values";
	}

}
