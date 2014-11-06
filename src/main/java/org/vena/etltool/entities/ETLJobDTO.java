package org.vena.etltool.entities;

import java.util.Date;

import org.vena.api.etl.ETLJob.Phase;
import org.vena.api.etl.ETLMetadata;
import org.vena.id.Id;

public class ETLJobDTO {

	private Id id;
	
	private ETLMetadata metadata;

	private Id requestId;

	private boolean cancelRequested;

	private boolean error;

	private String errorMessage;

	private String validationResults;

	private Phase phase;

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

	public ETLMetadata getMetadata() {
		return metadata;
	}

	public void setMetadata(ETLMetadata metadata) {
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
