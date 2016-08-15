package org.vena.etltool.entities;

public abstract class ETLStreamStepDTO extends ETLRowProcessingStepDTO {
	public enum MockMode {
		LIVE,
		MOCK
	}

	protected Id sourceId;
	protected MockMode mockMode;

	protected ETLStreamStepDTO() {}
	
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
