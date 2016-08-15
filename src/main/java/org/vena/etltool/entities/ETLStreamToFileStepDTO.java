package org.vena.etltool.entities;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLStreamToFileStepDTO.stepType)
public class ETLStreamToFileStepDTO extends ETLStreamStepDTO{
	protected final static String stepType = "ETLStreamToFileStep";
	
	public ETLStreamToFileStepDTO() {
	}

	@Override
	public String getName() {
		return "Streaming to file";
	}

}
