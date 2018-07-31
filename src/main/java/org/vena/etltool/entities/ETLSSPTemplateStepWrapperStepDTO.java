package org.vena.etltool.entities;

import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.List;

@JsonTypeName(ETLSSPTemplateStepWrapperStepDTO.stepType)
public class ETLSSPTemplateStepWrapperStepDTO extends ETLStepDTO {
	protected final static String stepType = "ETLSSPTemplateStepWrapperStep";

	private Id sspTemplateId;
	private String sspTemplateName;
	private List<ETLStepDTO> sspSteps;
	private int currentStep;

	public ETLSSPTemplateStepWrapperStepDTO() {
		super();
	}

	public ETLSSPTemplateStepWrapperStepDTO(Id sspTemplateId, String sspTemplateName, int currentStep) {
		this.sspTemplateId = sspTemplateId;
		this.sspTemplateName = sspTemplateName;
		this.currentStep = currentStep;
	}

	public Id getSspTemplateId() {
		return sspTemplateId;
	}

	public void setSspTemplateId(Id sspTemplateId) {
		this.sspTemplateId = sspTemplateId;
	}

	public String getSspTemplateName() {
		return sspTemplateName;
	}

	public void setSspTemplateName(String sspTemplateName) {
		this.sspTemplateName = sspTemplateName;
	}

	public List<ETLStepDTO> getSspSteps() {
		return sspSteps;
	}

	public void setSspSteps(List<ETLStepDTO> sspSteps) {
		this.sspSteps = sspSteps;
	}

	public int getCurrentStep() {
		return currentStep;
	}

	public void setCurrentStep(int currentStep) {
		this.currentStep = currentStep;
	}

	@Override
	public String getName() {
		return "SSP Template Automation";
	}
}
