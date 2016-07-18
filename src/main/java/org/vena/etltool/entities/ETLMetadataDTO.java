package org.vena.etltool.entities;

import java.util.ArrayList;
import java.util.List;

import org.vena.api.etl.ETLFileToCubeStep;
import org.vena.id.Id;

public class ETLMetadataDTO {
	
	public enum ETLLoadType {
		FILE_TO_CUBE,
		FILE_TO_STAGE_TO_CUBE,
		FILE_TO_STAGE,
		STAGE_TO_CUBE
	}
	
	public static String loadTypeToString(ETLLoadType loadType) {
		if (loadType == null) return null;

		switch (loadType) {
		case FILE_TO_CUBE:
			return "Direct Load";
		case FILE_TO_STAGE_TO_CUBE:
			return "Stage, Transform, and Load";
		case FILE_TO_STAGE:
			return "Stage Only";
		case STAGE_TO_CUBE:
			return "Load From Staging";
		default:
			return "Unrecognized Load Type";
		}
	}
	
	public ETLMetadataDTO() {
		super();
	}
	
	private Integer schemaVersion;
	
	private String name;
	
	private Id modelId;

	private ETLLoadType loadType;

	private List<ETLStepDTO> steps = new ArrayList<ETLStepDTO>();
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Id getModelId() {
		return modelId;
	}

	public void setModelId(Id modelId) {
		this.modelId = modelId;
	}
	
	public ETLLoadType getLoadType() {
		return loadType;
	}

	public void setLoadType(ETLLoadType loadType) {
		this.loadType = loadType;
	}

	public List<ETLStepDTO> getSteps() {
		return steps;
	}

	public void setSteps(List<ETLStepDTO> steps) {
		this.steps = steps;
	}
	
	public boolean addStep(ETLStepDTO step) {
		int n = steps.size();
		step.setStepNumber(n);
		return steps.add(step);
	}

	public Integer getSchemaVersion() {
		return schemaVersion;
	}
	
	public void setSchemaVersion(Integer schemaVersion) {
		this.schemaVersion = schemaVersion;
	}

	public void addStep(ETLFileToCubeStep etlFileToCubeStep) {
		// TODO Auto-generated method stub
		
	}
}
