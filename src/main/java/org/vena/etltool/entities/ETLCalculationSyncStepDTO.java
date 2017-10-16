package org.vena.etltool.entities;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLCalculationSyncStepDTO.stepType)
public class ETLCalculationSyncStepDTO extends ETLStepDTO {

	protected final static String stepType = "ETLCalculationSyncStep";

	@Override
	public String getName() {
		return "Calculating";
	}

}
