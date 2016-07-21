package org.vena.etltool.entities;

import java.util.Collection;
import java.util.Map;
import java.util.SortedMap;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLSQLTransformStepDTO.stepType)
public class ETLSQLTransformStepDTO extends ETLStepDTO{
	protected final static String stepType = "ETLSQLTransformStep";

	private boolean initComplete;

	private boolean transformComplete;

	public ETLSQLTransformStepDTO() {
		super();
	}

	public ETLSQLTransformStepDTO(SortedMap<String, ETLFileOldDTO> files, Map<String, ETLTableStatusDTO> tables) {
		this(files.values(), tables);
	}
	
	public ETLSQLTransformStepDTO(Collection<ETLFileOldDTO> files, Map<String, ETLTableStatusDTO> tables) {
		boolean filesDone = true;

		for(ETLFileOldDTO file : files) {
			if (!file.getDone()) {
				filesDone = false;
				break;
			}
		}
		if (filesDone) {
			if (tables != null) this.status = Status.COMPLETED;
			else this.status = Status.WAITING;
		}
		else this.status = Status.NOT_STARTED;
	}

	public boolean isInitComplete() {
		return initComplete;
	}

	public void setInitComplete(boolean initComplete) {
		this.initComplete = initComplete;
	}

	public boolean isTransformComplete() {
		return transformComplete;
	}

	public void setTransformComplete(boolean transformComplete) {
		this.transformComplete = transformComplete;
	}

	@Override
	public String getName() {
		return "SQL Transformation";
	}

}
