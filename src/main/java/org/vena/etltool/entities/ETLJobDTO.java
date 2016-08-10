package org.vena.etltool.entities;

import java.util.Date;

import org.vena.etltool.entities.ETLStepDTO.Status;

public class ETLJobDTO {
	
	public enum Phase { NOT_STARTED, LOAD_TO_STAGING, IN_STAGING, LOAD_TO_CUBE, COMPLETE, CLEARING, VERSIONING }

	private Id id;
	
	private ETLMetadataDTO metadata;

	private Id requestId;

	private boolean cancelRequested;

	private boolean error;

	private String errorMessage;

	private String validationResults;

	private Phase phase;
	
	private Status status;

	private Id templateId;

	private Date createdDate;

	private Date updatedDate;

	private Id userId;

	private UserDTO user;

	public ETLJobDTO() {
	}

	public Id getId() {
		return id;
	}

	public void setId(Id id) {
		this.id = id;
	}

	public ETLMetadataDTO getMetadata() {
		return metadata;
	}

	public void setMetadata(ETLMetadataDTO metadata) {
		this.metadata = metadata;
	}

	public Id getRequestId() {
		return requestId;
	}

	public void setRequestId(Id requestId) {
		this.requestId = requestId;
	}

	public boolean isCancelRequested() {
		return cancelRequested;
	}

	public void setCancelRequested(boolean cancelRequested) {
		this.cancelRequested = cancelRequested;
	}

	public boolean isError() {
		return error;
	}

	public void setError(boolean error) {
		this.error = error;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public String getValidationResults() {
		return validationResults;
	}

	public void setValidationResults(String validationResults) {
		this.validationResults = validationResults;
	}

	public Phase getPhase() {
		return phase;
	}

	public void setPhase(Phase phase) {
		this.phase = phase;
	}
	
	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public Id getTemplateId() {
		return templateId;
	}

	public void setTemplateId(Id templateId) {
		this.templateId = templateId;
	}

	public Date getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(Date createdDate) {
		this.createdDate = createdDate;
	}

	public Date getUpdatedDate() {
		return updatedDate;
	}

	public void setUpdatedDate(Date updatedDate) {
		this.updatedDate = updatedDate;
	}

	public Id getUserId() {
		return userId;
	}

	public void setUserId(Id userId) {
		this.userId = userId;
	}

	public UserDTO getUser() {
		return user;
	}

	public void setUser(UserDTO user) {
		this.user = user;
	}

}
