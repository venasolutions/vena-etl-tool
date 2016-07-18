package org.vena.etltool.entities;

public abstract class ETLVersioningStepDTO extends ETLStepDTO{

	private int sourceIntersectionsProcessed;

	public int getSourceIntersectionsProcessed() {
		return sourceIntersectionsProcessed;
	}

	public void setSourceIntersectionsProcessed(int sourceIntersectionsProcessed) {
		this.sourceIntersectionsProcessed = sourceIntersectionsProcessed;
	}

	public abstract String getName();
}
