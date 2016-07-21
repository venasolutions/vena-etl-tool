package org.vena.etltool.entities;

import org.vena.api.customer.etl.transform.RowSource;
import org.vena.id.Id;

public abstract class ETLStreamStepDTO extends ETLRowProcessingStepDTO {
	public enum MockMode {
		LIVE,
		MOCK
	}

	protected Id sourceId;
	protected MockMode mockMode;

	protected ETLStreamStepDTO() {}
	
	protected ETLStreamStepDTO(RowSource source, MockMode mockMode) {
		this.sourceId = source.getId();
		this.mockMode = mockMode;
	}

	public Id getSourceId() {
		return sourceId;
	}

	public void setSourceId(Id sourceId) {
		this.sourceId = sourceId;
	}

	public MockMode getMockMode() {
		return mockMode;
	}

	public void setMockMode(MockMode mockMode) {
		this.mockMode = mockMode;
	}

}
