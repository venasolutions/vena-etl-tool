package org.vena.etltool.entities;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class ETLMetadataDTO {
	
	public enum ETLLoadType {
		FILE_TO_CUBE,
		FILE_TO_STAGE_TO_CUBE,
		FILE_TO_STAGE,
		STAGE_TO_CUBE,
		FILE_TO_VENA_TABLE
	}

	public static ETLLoadType stagingRequiredToLoadType(boolean stagingRequired) {
		return stagingRequired ? ETLLoadType.FILE_TO_STAGE_TO_CUBE : ETLLoadType.FILE_TO_CUBE;
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
		case FILE_TO_VENA_TABLE:
			return "Load to Vena Table";
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
	
	private Id processId;

	private ETLLoadType loadType;

	private List<ETLStepDTO> steps = new ArrayList<ETLStepDTO>();
	
	private SortedMap<String, ETLFileOldDTO> files = null;

	private Map<String, ETLTableStatusDTO> tables;

	private boolean stagingRequired;

	private Boolean queuingEnabled = null;

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

	public Id getProcessId() {
		return processId;
	}

	public void setProcessId(Id processId) {
		this.processId = processId;
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
		
		resequenceSteps();
	}
	
	public void resequenceSteps() {
		int i=0;

		for(ETLStepDTO step : this.steps) {
			step.setStepNumber(i);
			++i;
		}
	}
	
	@JsonIgnore
	public List<ETLFileImportStepDTO> getAllFileSteps()
	{
		List<ETLFileImportStepDTO> fileSteps = new ArrayList<>();

		for (ETLStepDTO step : steps) {
			if (step instanceof ETLFileImportStepDTO) {
				fileSteps.add((ETLFileImportStepDTO) step);
			}
		}

		return fileSteps;
	}
	
	public SortedMap<String, ETLFileOldDTO> getFiles() {
		return files;
	}	

	public void setFiles(SortedMap<String, ETLFileOldDTO> files) {
		this.files = files;
	}

	public boolean isStagingRequired() {
		return stagingRequired;
	}

	public void setStagingRequired(boolean staging) {
		this.stagingRequired = staging;
	}

	public Map<String, ETLTableStatusDTO> getTables() {
		return tables;
	}

	public void setTables(Map<String, ETLTableStatusDTO> tables) {
		this.tables = tables;
	}
	
	public void addTable(ETLTableStatusDTO table) {
		if (tables == null) tables = new LinkedHashMap<>();
		tables.put(table.getTableName(), table);
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

	public Boolean getQueuingEnabled() {
		return queuingEnabled;
	}

	public void setQueuingEnabled(Boolean queuingEnabled) {
		this.queuingEnabled = queuingEnabled;
	}
}
