package org.vena.etltool.tests;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashSet;

import org.junit.Test;
import org.vena.etltool.ETLClient;
import org.vena.etltool.Main;
import org.vena.etltool.entities.*;
import org.vena.etltool.entities.ETLFileImportStepDTO.FileFormat;
import org.vena.etltool.entities.ETLStepDTO.DataType;

public class ETLToolLoadStepsTest extends ETLToolTest {
	
	@Test
	public void testLoadSteps() throws UnsupportedEncodingException {
		String testFile = "src" + File.separatorChar + "test" + File.separatorChar + "resources" + File.separatorChar + "steps.txt";
		String[] args = buildCommand(new String[] {"--jobName", "Loading job with multiple steps", "--loadSteps", testFile});
		ETLClient etlClient = mockETLClient();
		
		ETLMetadataDTO metadata = Main.buildETLMetadata(args, etlClient);
		assertEquals(6, metadata.getSteps().size());

		ETLStepDTO firstStep = metadata.getSteps().get(0);
		assertEquals(ETLFileToStageStepDTO.class, firstStep.getClass());

		ETLFileToStageStepDTO fileToStageStep = (ETLFileToStageStepDTO) firstStep;
		assertEquals(DataType.intersections, fileToStageStep.getDataType());
		assertEquals("intersectionsFile.csv", fileToStageStep.getFileName());
		assertEquals("values_table", fileToStageStep.getTableName());
		assertEquals(FileFormat.CSV, fileToStageStep.getFileFormat());
		
		ETLStepDTO secondStep = metadata.getSteps().get(1);
		assertEquals(ETLSQLTransformStepDTO.class, secondStep.getClass());
		
		ETLStepDTO thirdStep = metadata.getSteps().get(2);
		assertEquals(ETLStageToCubeStepDTO.class, thirdStep.getClass());

		ETLStageToCubeStepDTO stageToCubeThirdStep = (ETLStageToCubeStepDTO)thirdStep;
		assertEquals(DataType.intersections, stageToCubeThirdStep.getDataType());
		assertEquals(Arrays.asList("dimension('Accounts':'Expense')","dimension('Accounts':'Sale')"), stageToCubeThirdStep.getClearSlicesExpressions());

		ETLStepDTO fourthStep = metadata.getSteps().get(3);
		assertEquals(ETLStageToCubeStepDTO.class, fourthStep.getClass());

		ETLStageToCubeStepDTO stageToCubeFourthStep = (ETLStageToCubeStepDTO)fourthStep;
		assertEquals(DataType.intersections, stageToCubeFourthStep.getDataType());
		assertEquals(new HashSet<>(Arrays.asList(1, 3, 4)), stageToCubeFourthStep.getClearSlicesDimensions());

		ETLStepDTO fifthStep = metadata.getSteps().get(4);
		assertEquals(ETLFileToVenaTableStepDTO.class, fifthStep.getClass());

		ETLFileToVenaTableStepDTO fileToVenaTableFifthStep = (ETLFileToVenaTableStepDTO)fifthStep;
		assertEquals(DataType.intersections, fileToVenaTableFifthStep.getDataType());
		assertEquals("intersectionsFile.csv", fileToVenaTableFifthStep.getFileName());
		assertEquals("venaTable_values_table1", fileToVenaTableFifthStep.getTableName());
		assertEquals(FileFormat.CSV, fileToVenaTableFifthStep.getFileFormat());

		// This is actually wrong!
		// The file contains the line `clearSlicesByColumns="col1,col2"` 
		// This should be treated as one column, but we strip out the quotes BEFORE parsing the columns
		// TODO revisit this approach if/when it turns out customers actually have columns with commas (or semicolons!) in them 
		assertEquals(Arrays.asList("col1","col2"),(fileToVenaTableFifthStep).getClearSlicesColumns());

		ETLStepDTO sixthStep = metadata.getSteps().get(5);
		assertEquals(ETLFileToVenaTableStepDTO.class, sixthStep.getClass());

		ETLFileToVenaTableStepDTO fileToVenaTableSixthStep = (ETLFileToVenaTableStepDTO)sixthStep;
		assertEquals(DataType.intersections, fileToVenaTableSixthStep.getDataType());
		assertEquals("intersectionsFile.csv", fileToVenaTableSixthStep.getFileName());
		assertEquals("venaTable_values_table2", fileToVenaTableSixthStep.getTableName());
		assertEquals(FileFormat.CSV, fileToVenaTableSixthStep.getFileFormat());
	}
	
	@Test
	public void testLoadStepsMissingFile() throws UnsupportedEncodingException {
		String[] args = buildCommand(new String[] {"--jobName", "Loading job with multiple steps", "--loadSteps"});
		ETLClient etlClient = mockETLClient();
		
		try {
			Main.buildETLMetadata(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: Missing argument for option: loadSteps", err.toString().trim());
		}
	}

}
