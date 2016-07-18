package org.vena.etltool.entities;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLCloneDataModelStepDTO.stepType)
public class ETLCloneDataModelStepDTO extends ETLStepDTO {

	protected final static String stepType = "ETLCloneDataModelStep";
	
	private boolean attributesIncluded;
	
	private String modelName;
	
	private String modelDesc;

	public ETLCloneDataModelStepDTO() {
		super();
	}
	
	@Override
	public String getName() {
		return "Cloning Data Model";
	}

	public String getModelName() {
		return modelName;
	}

	public void setModelName(String modelName) {
		this.modelName = modelName;
	}

	public String getModelDesc() {
		return modelDesc;
	}

	public void setModelDesc(String modelDesc) {
		this.modelDesc = modelDesc;
	}

	public boolean isAttributesIncluded() {
		return attributesIncluded;
	}

	public void setAttributesIncluded(boolean attributesIncluded) {
		this.attributesIncluded = attributesIncluded;
	}
}
