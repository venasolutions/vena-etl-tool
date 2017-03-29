package org.vena.etltool.entities;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;

@JsonTypeInfo(use = com.fasterxml.jackson.annotation.JsonTypeInfo.Id.NAME, include = As.PROPERTY, property = "stepType")
@JsonSubTypes({
		@Type(value = ETLFileToCubeStepDTO.class),
		@Type(value = ETLFileToStageStepDTO.class),
		@Type(value = ETLCubeToStageStepDTO.class),
		@Type(value = ETLStageToCubeStepDTO.class),
		@Type(value = ETLStreamChannelStepDTO.class),
		@Type(value = ETLSQLTransformStepDTO.class),
		@Type(value = ETLDeleteIntersectionsStepDTO.class),
		@Type(value = ETLDeleteZeroIntersectionsStepDTO.class),
		@Type(value = ETLDeleteLidsStepDTO.class),
		@Type(value = ETLDeleteValuesStepDTO.class),
		@Type(value = ETLCloneDataModelStepDTO.class),
		@Type(value = ETLCopyIntersectionsStepDTO.class),
		@Type(value = ETLCopyLIDsStepDTO.class),
		@Type(value = ETLDeleteDimensionStepDTO.class),
		@Type(value = ETLAddDimensionStepDTO.class),
		@Type(value = ETLFileMigrationStepDTO.class),
		@Type(value = ETLAsyncSaveDataStepDTO.class),
		@Type(value = ETLVersioningCopyStepDTO.class),
		@Type(value = ETLVersioningClearStepDTO.class),
		@Type(value = ETLCalculationDeployStepDTO.class)
})

public abstract class ETLStepDTO {

	public enum Status { NOT_STARTED, RUNNING, COMPLETED, ERROR, CANCELLED, WAITING, QUEUED };
	
	public enum DataType { intersections, values, lids, hierarchy, dimensions, attributes, user_defined, intersection_members, lid_members, variables, setexpressions}

	protected Status status = Status.NOT_STARTED;
	
	private int stepNumber;

	private Date startedTS = null;

	private Date completedTS = null;

	protected int percentDone;
	
	public abstract String getName();
	
	public Status getStatus() {
		return status;
	}
	
	public int getStepNumber() {
		return stepNumber;
	}

	public void setStepNumber(int stepNumber) {
		this.stepNumber = stepNumber;
	}
	
	public Date getStartedTS() {
		return startedTS;
	}

	public Date getCompletedTS() {
		return completedTS;
	}
	
	public int getPercentDone() {
		return percentDone;
	}

	public void setPercentDone(int percentDone) {
		this.percentDone = percentDone;
	}

	public void setPercentDone(long n, long total) {
		if (total > 0) {
			this.percentDone = (int) (100 * n / total);
		}
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName() + " [stepNumber=" + stepNumber + ", status=" + status + ", startedTS=" + startedTS
				+ ", completedTS=" + completedTS + ", percentDone=" + percentDone + "]";
	}
}
