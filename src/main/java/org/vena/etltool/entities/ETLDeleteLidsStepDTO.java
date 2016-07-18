package org.vena.etltool.entities;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLDeleteLidsStepDTO.stepType)

public class ETLDeleteLidsStepDTO extends AbstractETLDeleteStepDTO {
	
	protected final static String stepType = "ETLDeleteLidsStep";

	public ETLDeleteLidsStepDTO() {
		super();
	}
	
	@Override
	public String getName() {
		return "Deleting Lids";
	}
}
