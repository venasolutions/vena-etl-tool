package org.vena.etltool.entities;

import org.vena.api.customer.etl.load.FileDestination;
import org.vena.api.customer.etl.transform.RowSource;

public class ETLStreamToFileStepDTO extends ETLStreamStepDTO{
	public ETLStreamToFileStepDTO() {
	}

	public ETLStreamToFileStepDTO(RowSource source, FileDestination destination, MockMode mockMode) {
		super(source, mockMode);
	}

	@Override
	public String getName() {
		return "Streaming to file";
	}

}
