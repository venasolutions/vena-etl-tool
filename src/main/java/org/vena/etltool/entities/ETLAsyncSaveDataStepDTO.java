package org.vena.etltool.entities;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLAsyncSaveDataStepDTO.stepType)
public class ETLAsyncSaveDataStepDTO extends ETLStepDTO {
	protected final static String stepType = "ETLAsyncSaveDataStep";

	List<Id> deleteLabelIds;

	Id saveId;
	Id modelId;
	Id contextId;
	
	public ETLAsyncSaveDataStepDTO () {
	}
	
	@Override
	public String getName() {
		return "Addin Save Data";
	}

	public List<Id> getDeleteLabelIds() {
		return deleteLabelIds;
	}

	public void setDeleteLabelIds(List<Id> deleteLabelIds) {
		this.deleteLabelIds = deleteLabelIds;
	}
	
	public Id getModelId() {
		return modelId;
	}

	public void setModelId(Id modelId) {
		this.modelId = modelId;
	}
	
	public Id getContextId() {
		return contextId;
	}

	public void setContextId(Id contextId) {
		this.contextId = contextId;
	}

	public Id getSaveId() {
		return saveId;
	}

	public void setSaveId(Id saveId) {
		this.saveId = saveId;
	}
}
