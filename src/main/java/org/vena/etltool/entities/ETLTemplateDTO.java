package org.vena.etltool.entities;

import java.util.Date;

public class ETLTemplateDTO {

	private Id id;
	
	private ETLMetadataDTO metadata;
	
	private Date lastRunTime;
	
	private boolean isRunning = false;
		
	public ETLTemplateDTO() {
	}

	/**
	 * @return the id
	 */
	public Id getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(Id id) {
		this.id = id;
	}

	public ETLMetadataDTO getMetadata() {
		return metadata;
	}

	public void setMetadata(ETLMetadataDTO metadata) {
		this.metadata = metadata;
	}

	public Date getLastRunTime() {
		return lastRunTime;
	}

	public void setLastRunTime(Date lastRunTime) {
		this.lastRunTime = lastRunTime;
	}
	
	public boolean isRunning() {
		return isRunning;
	}

	public void setRunning(boolean isRunning) {
		this.isRunning = isRunning;
	}
}
