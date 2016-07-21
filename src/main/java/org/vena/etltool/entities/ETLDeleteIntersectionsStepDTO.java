package org.vena.etltool.entities;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ETLDeleteIntersectionsStepDTO.stepType)
public class ETLDeleteIntersectionsStepDTO extends AbstractETLDeleteStepDTO {
	protected final static String stepType = "ETLDeleteIntersectionsStep";

	public ETLDeleteIntersectionsStepDTO() {
		super();
	}
	
	@Override
	public String getName() {
		return "Deleting Intersections";
	}
}
