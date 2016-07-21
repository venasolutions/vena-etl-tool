package org.vena.etltool.entities;

import org.vena.id.Id;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLAddDimensionStepDTO.stepType)
public class ETLAddDimensionStepDTO extends ETLStepDTO {

	protected final static String stepType = "ETLAddDimensionStep";
	
	private String dimName;
	private Id id;
	private Id defaultMemberId;
	private int resumeLine;
	
	public ETLAddDimensionStepDTO() {
		super();
	}
	
	public ETLAddDimensionStepDTO(Id id, String dimName, Id defaultMemberId) {
		super();
		
		this.dimName = dimName;
		this.id = id;
		this.defaultMemberId = defaultMemberId;
	}
	
	public Id getId() {
		return id;
	}
	
	public void setId(Id id) {
		this.id = id;
	}
	
	public Id getDefaultMemberId() {
		return defaultMemberId;
	}

	public void setDefaultMemberId(Id defaultMemberId) {
		this.defaultMemberId = defaultMemberId;
	}

	public String getDimensionName() {
		return dimName;
	}
	
	public void setDimensionName(String dimName) {
		this.dimName = dimName;
	}
	
	@Override
	public String getName() {
		return "Adding Dimensions";
	}
	
	public int getResumeLine() {
		return resumeLine;
	}

	public void setResumeLine(int resumeLine) {
		this.resumeLine = resumeLine;
	}
}
