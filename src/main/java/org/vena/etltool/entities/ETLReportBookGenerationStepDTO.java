package org.vena.etltool.entities;

import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.List;
import java.util.Map;

@JsonTypeName(ETLReportBookGenerationStepDTO.stepType)
public class ETLReportBookGenerationStepDTO extends ETLStepDTO {
    protected final static String stepType = "ETLReportBookGenerationStep";

    public String name;
    public String reportBookName;
    public Id reportTaskId;
    public String taskName;
    public String processName;
    public Id reportBookId;
    public Id fileId;
    public Id modelId;
    public Map<String, Object[]> pageOptionsList;
    public List<Id> users;
    public List<Id> generatedBookEntries;

    @Override
    public String getName() {
        return "ETLReportBookGenerationStep";
    }
}