package org.vena.etltool.tests;

import static org.junit.Assert.assertEquals;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.Test;
import org.vena.etltool.ETLClient;
import org.vena.etltool.Main;
import org.vena.etltool.entities.ETLFileToStageStepDTO;
import org.vena.etltool.entities.ETLMetadataDTO;
import org.vena.etltool.entities.ETLSQLTransformStepDTO;
import org.vena.etltool.entities.ETLStageToCubeStepDTO;
import org.vena.etltool.entities.ETLStepDTO;
import org.vena.etltool.entities.ETLFileImportStepDTO.FileFormat;
import org.vena.etltool.entities.ETLStepDTO.DataType;

public class ETLToolFileToStageToCubeTest extends ETLToolTest {
	
	@Test
	public void testFileToStageToCube() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Loading intersections file with transform", "--file", "intersectionsFile.csv;type=intersections;format=CSV;table=values_table", "--stageAndTransform"});

		ETLMetadataDTO metadata = Main.parseCmdlineArgs(args, etlClient);

		assertEquals(modelId, metadata.getModelId());
		assertEquals("Loading intersections file with transform", metadata.getName());
		assertEquals(6, metadata.getSteps().size());
		
		List<ETLStepDTO> steps = metadata.getSteps();
		ETLStepDTO firstStep = steps.get(0);
		ETLStepDTO secondStep = steps.get(1);
		
		assertEquals(ETLFileToStageStepDTO.class, firstStep.getClass());
		assertEquals(ETLSQLTransformStepDTO.class, secondStep.getClass());

		ETLFileToStageStepDTO fileToStageStep = (ETLFileToStageStepDTO) firstStep;
		assertEquals(DataType.intersections, fileToStageStep.getDataType());
		assertEquals("intersectionsFile.csv", fileToStageStep.getFileName());
		assertEquals("values_table", fileToStageStep.getTableName());
		assertEquals(FileFormat.CSV, fileToStageStep.getFileFormat());
		
		List<ETLStepDTO> stageToCubeSteps = steps.subList(steps.size() - 4, steps.size());
		List<DataType> types = Arrays.asList(DataType.hierarchy, DataType.attributes, DataType.intersections, DataType.lids);

		for (int i=0; i < stageToCubeSteps.size(); i++) { 
			assertEquals(ETLStageToCubeStepDTO.class, stageToCubeSteps.get(i).getClass());
			DataType type = ((ETLStageToCubeStepDTO)stageToCubeSteps.get(i)).getDataType();
			assertEquals(types.get(i), type);
		}
	}
	
	@Test
	public void testFileToStageToCubeWithClearSlices() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Loading intersections file with transform", "--file", "intersectionsFile.csv;type=intersections;format=CSV;table=values_table", "--stage", "--clearSlices", "dimension('Accounts':'Expense'),dimension('Accounts':'Sale')"});

		ETLMetadataDTO metadata = Main.parseCmdlineArgs(args, etlClient);

		assertEquals(modelId, metadata.getModelId());
		assertEquals("Loading intersections file with transform", metadata.getName());
		assertEquals(6, metadata.getSteps().size());
		
		List<ETLStepDTO> steps = metadata.getSteps();
		ETLStepDTO firstStep = steps.get(0);
		ETLStepDTO secondStep = steps.get(1);
		
		assertEquals(ETLFileToStageStepDTO.class, firstStep.getClass());
		assertEquals(ETLSQLTransformStepDTO.class, secondStep.getClass());

		ETLFileToStageStepDTO fileToStageStep = (ETLFileToStageStepDTO) firstStep;
		assertEquals(DataType.intersections, fileToStageStep.getDataType());
		assertEquals("intersectionsFile.csv", fileToStageStep.getFileName());
		assertEquals("values_table", fileToStageStep.getTableName());
		assertEquals(FileFormat.CSV, fileToStageStep.getFileFormat());
		
		List<ETLStepDTO> stageToCubeSteps = steps.subList(steps.size() - 4, steps.size());
		List<DataType> types = Arrays.asList(DataType.hierarchy, DataType.attributes, DataType.intersections, DataType.lids);

		for (int i=0; i < stageToCubeSteps.size(); i++) { 
			assertEquals(ETLStageToCubeStepDTO.class, stageToCubeSteps.get(i).getClass());
			DataType type = ((ETLStageToCubeStepDTO)stageToCubeSteps.get(i)).getDataType();
			assertEquals(types.get(i), type);
			if (type.equals(DataType.intersections)) {
				assertEquals(Arrays.asList("dimension('Accounts':'Expense')","dimension('Accounts':'Sale')"), ((ETLStageToCubeStepDTO)stageToCubeSteps.get(i)).getClearSlicesExpressions());
			}
		}
	}

	@Test
	public void testFileToStageToCubeWithClearSlicesByDimNums() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Loading intersections file with transform", "--file", "intersectionsFile.csv;type=intersections;format=CSV;table=values_table", "--stage", "--clearSlicesByDimNums", "1,3,4"});

		ETLMetadataDTO metadata = Main.parseCmdlineArgs(args, etlClient);

		assertEquals(modelId, metadata.getModelId());
		assertEquals("Loading intersections file with transform", metadata.getName());
		assertEquals(6, metadata.getSteps().size());

		List<ETLStepDTO> steps = metadata.getSteps();
		ETLStepDTO firstStep = steps.get(0);
		ETLStepDTO secondStep = steps.get(1);

		assertEquals(ETLFileToStageStepDTO.class, firstStep.getClass());
		assertEquals(ETLSQLTransformStepDTO.class, secondStep.getClass());

		ETLFileToStageStepDTO fileToStageStep = (ETLFileToStageStepDTO) firstStep;
		assertEquals(DataType.intersections, fileToStageStep.getDataType());
		assertEquals("intersectionsFile.csv", fileToStageStep.getFileName());
		assertEquals("values_table", fileToStageStep.getTableName());
		assertEquals(FileFormat.CSV, fileToStageStep.getFileFormat());

		List<ETLStepDTO> stageToCubeSteps = steps.subList(steps.size() - 4, steps.size());
		List<DataType> types = Arrays.asList(DataType.hierarchy, DataType.attributes, DataType.intersections, DataType.lids);

		for (int i=0; i < stageToCubeSteps.size(); i++) {
			assertEquals(ETLStageToCubeStepDTO.class, stageToCubeSteps.get(i).getClass());
			DataType type = ((ETLStageToCubeStepDTO)stageToCubeSteps.get(i)).getDataType();
			assertEquals(types.get(i), type);
			if (type.equals(DataType.intersections)) {
				assertEquals(new HashSet<Integer>(Arrays.asList(1, 3, 4)), ((ETLStageToCubeStepDTO)stageToCubeSteps.get(i)).getClearSlicesDimensions());
			}
		}
	}
}
