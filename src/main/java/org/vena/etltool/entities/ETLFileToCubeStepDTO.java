package org.vena.etltool.entities;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLFileToCubeStepDTO.stepType)

public class ETLFileToCubeStepDTO extends ETLFileImportStepDTO{
	protected final static String stepType = "ETLFileToCubeStep";

	private boolean createUnmappedMembers = true;
	
	private List<String> clearSlicesExpressions = null;
	
	private long seqNum;
	
	private long numDeleted;
	
	private long numLidsDeleted;

	public ETLFileToCubeStepDTO() {
	}

	public ETLFileToCubeStepDTO(ETLFileOldDTO etlFile) {
		super(etlFile);
		this.clearSlicesExpressions = etlFile.getClearSlicesExpressions();
	}

	@Override
	public String getName() {
		return "Importing File \""+getFileName()+"\" ("+getDataType()+")";
	}

	public boolean isCreateUnmappedMembers() {
		return createUnmappedMembers;
	}

	public void setCreateUnmappedMembers(boolean createUnmappedMembers) {
		this.createUnmappedMembers = createUnmappedMembers;
	}
	
	public void setClearSlicesExpressions(List<String> clearSlicesExpressions) {
		this.clearSlicesExpressions = clearSlicesExpressions;
	}
	
	public List<String> getClearSlicesExpressions() {
		return clearSlicesExpressions;
	}
	
	public long getSeqNum() {
		return seqNum;
	}
	
	public void setSeqNum(long seqNum) {
		this.seqNum = seqNum;
	}
	
	public void setNumDeleted(long numDeleted) {
		this.numDeleted = numDeleted;
	}

	public long getNumDeleted() {
		return numDeleted;
	}

	public void setNumLidsDeleted(long numLidsDeleted) {
		this.numLidsDeleted = numLidsDeleted;
	}

	public long getNumLidsDeleted() {
		return numLidsDeleted;
	}
}
