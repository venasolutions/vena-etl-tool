package org.vena.etltool.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashSet;

import org.junit.Test;
import org.vena.etltool.ETLClient;
import org.vena.etltool.Main;
import org.vena.etltool.entities.ETLFileImportStepDTO.FileFormat;
import org.vena.etltool.entities.ETLFileToCubeStepDTO;
import org.vena.etltool.entities.ETLMetadataDTO;
import org.vena.etltool.entities.ETLStepDTO;
import org.vena.etltool.entities.ETLStepDTO.DataType;

public class ETLToolFileToCubeTest extends ETLToolTest {

	@Test
	public void testFileToCube() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Loading LIDs file", "--file", "lidsFile.csv;type=lids;format=CSV;"});
		
		ETLMetadataDTO metadata = Main.parseCmdlineArgs(args, etlClient);
		
		assertEquals(modelId, metadata.getModelId());
		assertEquals("Loading LIDs file", metadata.getName());
		assertEquals(1, metadata.getSteps().size());
		
		ETLStepDTO step = metadata.getSteps().get(0);
		
		assertEquals(ETLFileToCubeStepDTO.class, step.getClass());
		
		ETLFileToCubeStepDTO fileToCubeStep = (ETLFileToCubeStepDTO)step;
		assertEquals(DataType.lids, fileToCubeStep.getDataType());
		assertEquals("lidsFile.csv", fileToCubeStep.getFileName());
		assertEquals(FileFormat.CSV, fileToCubeStep.getFileFormat());
	}
	
	@Test
	public void testFileToCubeWithClearSlices() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Loading intersections file", "--file", "intersectionsFile.csv;type=intersections;format=CSV;clearSlices=dimension('Accounts':'Sales'),dimension('Accounts':'Expense')"});
		
		ETLMetadataDTO metadata = Main.parseCmdlineArgs(args, etlClient);
		
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
	public void testFileToCubeWithIncorrectClearSlices() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Loading intersections file", "--file", "intersectionsFile.csv;type=intersections;format=CSV", "--clearSlices","dimension('Accounts':'Sales'),dimension('Accounts':'Expense')"});
		
		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: --clearSlices and --clearSlicesByDimNums options cannot be combined with the --file option. Instead use the suboptions clearSlices and clearSlicesByDimNums for the --file option\n", err.toString());
		}
	}

	@Test
	public void testFileToCubeWithClearSlicesByDimNums() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Loading intersections file", "--file", "intersectionsFile.csv;type=intersections;format=CSV;clearSlicesByDimNums=1,2,5"});

		ETLMetadataDTO metadata = Main.parseCmdlineArgs(args, etlClient);

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

		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			String expectedError = "Error: --clearSlices and --clearSlicesByDimNums options cannot be combined. At most one of these options can be used at a time.";
			assertEquals(1, e.status);
			assertTrue(err.toString().contains(expectedError));
		}
	}
}