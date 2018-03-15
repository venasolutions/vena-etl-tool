package org.vena.etltool.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.Test;
import org.vena.etltool.ETLClient;
import org.vena.etltool.Main;
import org.vena.etltool.entities.ETLFileToCubeStepDTO;
import org.vena.etltool.entities.ETLFileToStageStepDTO;
import org.vena.etltool.entities.ETLMetadataDTO;
import org.vena.etltool.entities.ETLSQLTransformStepDTO;
import org.vena.etltool.entities.ETLStageToCubeStepDTO;
import org.vena.etltool.entities.ETLStepDTO;
import org.vena.etltool.entities.ETLFileImportStepDTO.FileFormat;
import org.vena.etltool.entities.ETLStepDTO.DataType;

public class ETLToolClearSlicesTest extends ETLToolTest {
	
	@Test
	public void testFileToCubeWithClearSlices() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Loading intersections file", "--file", "intersectionsFile.csv;type=intersections;format=CSV;clearSlices=dimension('Accounts':'Sales'),dimension('Accounts':'Expense')"});
		
		ETLMetadataDTO metadata = Main.buildETLMetadata(args, etlClient);
		
		assertEquals(modelId, metadata.getModelId());
		assertEquals("Loading intersections file", metadata.getName());
		assertEquals(1, metadata.getSteps().size());
		
		ETLStepDTO step = metadata.getSteps().get(0);
		
		assertEquals(ETLFileToCubeStepDTO.class, step.getClass());
		
		ETLFileToCubeStepDTO fileToCubeStep = (ETLFileToCubeStepDTO)step;
		assertEquals(DataType.intersections, fileToCubeStep.getDataType());
		assertEquals("intersectionsFile.csv", fileToCubeStep.getFileName());
		assertEquals(FileFormat.CSV, fileToCubeStep.getFileFormat());
		assertEquals(Arrays.asList("dimension('Accounts':'Sales')","dimension('Accounts':'Expense')"),((ETLFileToCubeStepDTO)step).getClearSlicesExpressions());
	}

	@Test
	public void testFileToCubeWithClearSlicesByDimNums() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Loading intersections file", "--file", "intersectionsFile.csv;type=intersections;format=CSV;clearSlicesByDimNums=1,2,5"});

		ETLMetadataDTO metadata = Main.buildETLMetadata(args, etlClient);

		assertEquals(modelId, metadata.getModelId());
		assertEquals("Loading intersections file", metadata.getName());
		assertEquals(1, metadata.getSteps().size());

		ETLStepDTO step = metadata.getSteps().get(0);

		assertEquals(ETLFileToCubeStepDTO.class, step.getClass());

		ETLFileToCubeStepDTO fileToCubeStep = (ETLFileToCubeStepDTO)step;
		assertEquals(DataType.intersections, fileToCubeStep.getDataType());
		assertEquals("intersectionsFile.csv", fileToCubeStep.getFileName());
		assertEquals(FileFormat.CSV, fileToCubeStep.getFileFormat());
		assertEquals(new HashSet<Integer>(Arrays.asList(1, 2, 5)),((ETLFileToCubeStepDTO)step).getClearSlicesDimensions());
	}

	@Test
	public void testFileToCubeWithBothClearSlices() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Loading intersections file", "--file", "intersectionsFile.csv;type=intersections;format=CSV;clearSlicesByDimNums=1,2,5;clearSlices=dimension('Accounts':'Sales')"});

		ETLMetadataDTO metadata = Main.buildETLMetadata(args, etlClient);

		assertEquals(modelId, metadata.getModelId());
		assertEquals("Loading intersections file", metadata.getName());
		assertEquals(1, metadata.getSteps().size());

		ETLStepDTO step = metadata.getSteps().get(0);

		assertEquals(ETLFileToCubeStepDTO.class, step.getClass());

		ETLFileToCubeStepDTO fileToCubeStep = (ETLFileToCubeStepDTO)step;
		assertEquals(DataType.intersections, fileToCubeStep.getDataType());
		assertEquals("intersectionsFile.csv", fileToCubeStep.getFileName());
		assertEquals(FileFormat.CSV, fileToCubeStep.getFileFormat());
		assertEquals(new HashSet<Integer>(Arrays.asList(1, 2, 5)),((ETLFileToCubeStepDTO)step).getClearSlicesDimensions());
		assertEquals(Arrays.asList("dimension('Accounts':'Sales')"),((ETLFileToCubeStepDTO)step).getClearSlicesExpressions());
	}

	@Test
	public void testFileToCubeWithIncorrectClearSlices() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Loading intersections file", "--file", "intersectionsFile.csv;type=intersections;format=CSV", "--clearSlices","dimension('Accounts':'Sales'),dimension('Accounts':'Expense')"});
		
		try {
			Main.buildETLMetadata(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: --clearSlices and --clearSlicesByDimNums options cannot be combined with the --file option. Instead use the suboptions clearSlices and clearSlicesByDimNums for the --file option", err.toString().trim());
		}
	}

	@Test
	public void testStageToCubeWithClearSlices() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Loading to stage with clear", "--loadFromStaging", "--clearSlices", "dimension('Accounts':'Expense'),dimension('Accounts':'Sale')"});
		
		ETLMetadataDTO metadata = Main.buildETLMetadata(args, etlClient);
		
		assertEquals(modelId, metadata.getModelId());
		assertEquals("Loading to stage with clear", metadata.getName());
		assertEquals(4, metadata.getSteps().size());
		
		List<ETLStepDTO> steps = metadata.getSteps();
		List<DataType> types = Arrays.asList(DataType.hierarchy, DataType.attributes, DataType.intersections, DataType.lids);
		
		for (int i=0; i<steps.size(); i++) { 
			assertEquals(ETLStageToCubeStepDTO.class, steps.get(i).getClass());
			DataType type = ((ETLStageToCubeStepDTO)steps.get(i)).getDataType();
			assertEquals(types.get(i), type);
			if (type.equals(DataType.intersections)) {
				assertEquals(Arrays.asList("dimension('Accounts':'Expense')","dimension('Accounts':'Sale')"), ((ETLStageToCubeStepDTO)steps.get(i)).getClearSlicesExpressions());
			}
		}
	}

	@Test
	public void testStageToCubeWithClearSlicesByDimNums() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Loading to stage with clear", "--loadFromStaging", "--clearSlicesByDimNums", "3, 4"});

		ETLMetadataDTO metadata = Main.buildETLMetadata(args, etlClient);

		assertEquals(modelId, metadata.getModelId());
		assertEquals("Loading to stage with clear", metadata.getName());
		assertEquals(4, metadata.getSteps().size());

		List<ETLStepDTO> steps = metadata.getSteps();
		List<DataType> types = Arrays.asList(DataType.hierarchy, DataType.attributes, DataType.intersections, DataType.lids);

		for (int i=0; i<steps.size(); i++) {
			assertEquals(ETLStageToCubeStepDTO.class, steps.get(i).getClass());
			DataType type = ((ETLStageToCubeStepDTO)steps.get(i)).getDataType();
			assertEquals(types.get(i), type);
			if (type.equals(DataType.intersections)) {
				assertEquals(new HashSet<Integer>(Arrays.asList(3, 4)), ((ETLStageToCubeStepDTO)steps.get(i)).getClearSlicesDimensions());
			}
		}
	}

	@Test
	public void testStageToCubeWithBothClearSlices() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Loading to stage with clear", "--loadFromStaging", "--clearSlicesByDimNums", "3, 4", "--clearSlices", "dimension('Accounts':'Expense')"});

		ETLMetadataDTO metadata = Main.buildETLMetadata(args, etlClient);

		assertEquals(modelId, metadata.getModelId());
		assertEquals("Loading to stage with clear", metadata.getName());
		assertEquals(4, metadata.getSteps().size());

		List<ETLStepDTO> steps = metadata.getSteps();
		List<DataType> types = Arrays.asList(DataType.hierarchy, DataType.attributes, DataType.intersections, DataType.lids);

		for (int i=0; i<steps.size(); i++) {
			assertEquals(ETLStageToCubeStepDTO.class, steps.get(i).getClass());
			DataType type = ((ETLStageToCubeStepDTO)steps.get(i)).getDataType();
			assertEquals(types.get(i), type);
			if (type.equals(DataType.intersections)) {
				assertEquals(new HashSet<Integer>(Arrays.asList(3, 4)), ((ETLStageToCubeStepDTO)steps.get(i)).getClearSlicesDimensions());
				assertEquals(Arrays.asList("dimension('Accounts':'Expense')"), ((ETLStageToCubeStepDTO)steps.get(i)).getClearSlicesExpressions());
			}
		}
	}

	@Test
	public void testFileToStageToCubeWithClearSlices() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Loading intersections file with transform", "--file", "intersectionsFile.csv;type=intersections;format=CSV;table=values_table", "--stage", "--clearSlices", "dimension('Accounts':'Expense'),dimension('Accounts':'Sale')"});

		ETLMetadataDTO metadata = Main.buildETLMetadata(args, etlClient);

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

		ETLMetadataDTO metadata = Main.buildETLMetadata(args, etlClient);

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

	@Test
	public void testFileToStageToCubeWithBothClearSlices() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Loading intersections file with transform", "--file",
				"intersectionsFile.csv;type=intersections;format=CSV;table=values_table", "--stage",
				"--clearSlicesByDimNums", "1,3,4", "--clearSlices", "dimension('Accounts':'Expense'),dimension('Accounts':'Sale')"});

		ETLMetadataDTO metadata = Main.buildETLMetadata(args, etlClient);

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
				assertEquals(Arrays.asList("dimension('Accounts':'Expense')","dimension('Accounts':'Sale')"), ((ETLStageToCubeStepDTO)stageToCubeSteps.get(i)).getClearSlicesExpressions());
			}
		}
	}
}
