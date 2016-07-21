package org.vena.etltool.entities;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLDeleteDimensionStepDTO.stepType)
public class ETLDeleteDimensionStepDTO extends ETLStepDTO {

	protected final static String stepType = "ETLDeleteDimensionStep";
	
	private int dimNum;
	private int resumeLine;
	
	public ETLDeleteDimensionStepDTO() {
		super();
	}
	
	public ETLDeleteDimensionStepDTO(int dimNum) {
		super();
		this.dimNum = dimNum;
	}
	
	public int getDimensionNumber() {
		return dimNum;
	}
	
	public void setDimensionNumber(int dimNum) {
		this.dimNum = dimNum;
	}
	
	@Override
	public String getName() {
		return "Deleting Dimensions";
	}

	public int getResumeLine() {
		return resumeLine;
	}

	public void setResumeLine(int resumeLine) {
		this.resumeLine = resumeLine;
	}
}
