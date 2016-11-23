package org.vena.etltool.entities;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLStreamChannelStepDTO.stepType)

public class ETLStreamChannelStepDTO extends ETLStreamStepDTO {
	
	protected final static String stepType = "ETLStreamChannelStep";
		
	public ETLStreamChannelStepDTO() {
	}
	
	public ETLStreamChannelStepDTO(Id sourceId, MockMode mockMode) {
		this.sourceId = sourceId;
		this.mockMode = mockMode;
	}
	
	@Override
	public String getName() {
		return "Running integration channel: " + sourceId + ".";
	}

}
