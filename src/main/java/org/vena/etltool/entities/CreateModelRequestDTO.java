package org.vena.etltool.entities;

public class CreateModelRequestDTO {

	private String name;
	
	private String desc;
	
	public CreateModelRequestDTO()
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
