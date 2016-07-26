package org.vena.etltool.entities;

import org.vena.id.Id;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLCalculationDeployStepDTO.stepType)
public class ETLCalculationDeployStepDTO extends ETLStepDTO{

	protected final static String stepType = "ETLCalculationDeployStep";
	
	private Id calculationId;

	private String calculationName;

	private int sourceIntersectionsProcessed;

	public ETLCalculationDeployStepDTO() {
		super();
	}
	
	public ETLCalculationDeployStepDTO(Id calculationId, String calculationName) {
		this.calculationId = calculationId;
		this.calculationName = calculationName;
	}
	
	public Id getCalculationId() {
		return calculationId;
	}

	public void setCalculationId(Id calculationId) {
		this.calculationId = calculationId;
	}

	public String getCalculationName() {
		return calculationName;
	}

	public void setCalculationName(String calculationName) {
		this.calculationName = calculationName;
	}

	public int getSourceIntersectionsProcessed() {
		return sourceIntersectionsProcessed;
	}

	public void setSourceIntersectionsProcessed(int sourceIntersectionsProcessed) {
		this.sourceIntersectionsProcessed = sourceIntersectionsProcessed;
	}

	@Override
	public String getName() {		
		return "Calculating";
	}
}
