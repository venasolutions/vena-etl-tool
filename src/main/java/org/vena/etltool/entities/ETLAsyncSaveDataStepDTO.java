package org.vena.etltool.entities;

import java.util.List;

import org.vena.api.customer.datamodel.Intersection;
import org.vena.api.customer.datamodel.LIDLabel;
import org.vena.api.customer.datamodel.Variable;
import org.vena.id.Id;

public class ETLAsyncSaveDataStepDTO extends ETLStepDTO {

	List<Intersection> intersections;
	List<Variable> newRefVars;
	List<Variable> updateRefVars;
	List<LIDLabel> labels;
	List<Intersection> lids;
	List<Id> deleteLabelIds;

	Id saveId;
	Id modelId;
	Id contextId;
	
	public ETLAsyncSaveDataStepDTO () {
	}
	
	public ETLAsyncSaveDataStepDTO (List<Intersection> intersections, List<Variable> newRefVars, 
								List<Variable> updateRefVars, List<LIDLabel> labels, 
								List<Intersection> lids, List<Id> deleteLabelIds, Id modelId, Id contextId, Id saveId) {
		this.intersections = intersections;
		this.newRefVars = newRefVars;
		this.updateRefVars = updateRefVars;
		this.labels = labels;
		this.lids = lids;
		this.deleteLabelIds = deleteLabelIds;
		this.modelId = modelId;
		this.contextId = contextId;
		this.saveId = saveId;
	}

	@Override
	public String getName() {
		return "Addin Save Data";
	}

	public List<Intersection> getIntersections() {
		return intersections;
	}

	public void setIntersections(List<Intersection> intersections) {
		this.intersections = intersections;
	}

	public List<Variable> getNewRefVars() {
		return newRefVars;
	}

	public void setNewRefVars(List<Variable> newRefVars) {
		this.newRefVars = newRefVars;
	}

	public List<Variable> getUpdateRefVars() {
		return updateRefVars;
	}

	public void setUpdateRefVars(List<Variable> updateRefVars) {
		this.updateRefVars = updateRefVars;
	}

	public List<LIDLabel> getLabels() {
		return labels;
	}

	public void setLabels(List<LIDLabel> labels) {
		this.labels = labels;
	}

	public List<Intersection> getLids() {
		return lids;
	}

	public void setLids(List<Intersection> lids) {
		this.lids = lids;
	}
	
	public List<Id> getDeleteLabelIds() {
		return deleteLabelIds;
	}

	public void setDeleteLabelIds(List<Id> deleteLabelIds) {
		this.deleteLabelIds = deleteLabelIds;
	}
	
	public Id getModelId() {
		return modelId;
	}

	public void setModelId(Id modelId) {
		this.modelId = modelId;
	}
	
	public Id getContextId() {
		return contextId;
	}

	public void setContextId(Id contextId) {
		this.contextId = contextId;
	}

	public Id getSaveId() {
		return saveId;
	}

	public void setSaveId(Id saveId) {
		this.saveId = saveId;
	}
}
