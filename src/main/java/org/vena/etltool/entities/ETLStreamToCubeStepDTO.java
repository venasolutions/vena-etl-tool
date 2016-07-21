package org.vena.etltool.entities;

import org.vena.api.customer.etl.load.CubeDestination;
import org.vena.api.customer.etl.transform.RowSource;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLStreamToCubeStepDTO.stepType)
public class ETLStreamToCubeStepDTO extends ETLTypedStreamStepDTO{
	protected final static String stepType = "ETLStreamToCubeStep";
	
	String aggregationMode;
	
	public ETLStreamToCubeStepDTO() {
		super();
	}

	ETLStreamToCubeStepDTO(RowSource source, CubeDestination destination, MockMode mockMode) {
		super(source, mockMode);
		this.aggregationMode = destination.getAggregationMode();
		if (source.getDataType() == null)
			this.type = DataType.user_defined;
		else
			this.type = DataType.valueOf(source.getDataType());
	}

	@Override
	public String getName() {
		return "Streaming ("+getDataType()+")";
	}

	public String getAggregationMode() {
		return aggregationMode;
	}
}
