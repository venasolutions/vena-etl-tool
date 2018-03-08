package org.vena.etltool.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;

import org.junit.Test;
import org.vena.etltool.ETLClient;
import org.vena.etltool.Main;
import org.vena.etltool.entities.ETLFileImportStepDTO.FileFormat;
import org.vena.etltool.entities.ETLFileOldDTO;
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
	public void testFileToCubeWithEncoding() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Loading LIDs file", "--file", "lidsFile.csv;type=lids;format=CSV;encoding=UTF-16"});
		
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
		assertEquals("UTF-16", fileToCubeStep.getFileEncoding());
	}

	@Test
	public void testFileToCubeWithInvalidDataType() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Loading intersections file", "--file", "intersectionsFile.csv;type=staging;format=CSV;"});

		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			String expectedError = "The ETL file type \"staging\" does not exist. The supported filetypes are ["+ETLFileOldDTO.SUPPORTED_FILETYPES_LIST+"]";
			assertEquals(1, e.status);
			assertTrue(err.toString().contains(expectedError));
		}
	}
}