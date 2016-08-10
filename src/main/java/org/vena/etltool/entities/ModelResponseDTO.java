package org.vena.etltool.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ModelResponseDTO {
	Id id;
	
	private String name;
	
	private String desc;
	

	public Id getId() {
		return id;
	}

	public void setId(Id id) {
		this.id = id;
	}

	public ModelResponseDTO()
	{
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}
	
	
}
