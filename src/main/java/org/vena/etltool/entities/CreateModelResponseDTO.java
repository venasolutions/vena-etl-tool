package org.vena.etltool.entities;

import org.vena.id.Id;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateModelResponseDTO {
	Id id;
	
	private String name;
	
	private String desc;
	

	public Id getId() {
		return id;
	}

	public void setId(Id id) {
		this.id = id;
	}

	public CreateModelResponseDTO()
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
