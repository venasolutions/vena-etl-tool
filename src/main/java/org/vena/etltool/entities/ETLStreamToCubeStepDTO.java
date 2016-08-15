package org.vena.etltool.entities;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLStreamToCubeStepDTO.stepType)
public class ETLStreamToCubeStepDTO extends ETLTypedStreamStepDTO{
	protected final static String stepType = "ETLStreamToCubeStep";
	
	String aggregationMode;
	
	public ETLStreamToCubeStepDTO() {
		super();
	}

	@Override
	public String getName() {
		return "Streaming ("+getDataType()+")";
	}

	public String getAggregationMode() {
		return aggregationMode;
	}
}
