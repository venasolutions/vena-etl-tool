package org.vena.etltool.tests;

import org.junit.Test;
import org.vena.etltool.ETLClient;
import org.vena.etltool.Main;
import org.vena.etltool.entities.ETLFileImportStepDTO.FileFormat;
import org.vena.etltool.entities.ETLFileToRedshiftStepDTO;
import org.vena.etltool.entities.ETLMetadataDTO;
import org.vena.etltool.entities.ETLStepDTO;
import org.vena.etltool.entities.ETLStepDTO.DataType;

import java.io.UnsupportedEncodingException;

import static org.junit.Assert.assertEquals;

public class ETLToolFileToRedshiftTest extends ETLToolTest {

	@Test
	public void testFileToRedshift() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Loading LIDs file", "--file", "lidsFile.csv;type=lids;format=CSV;table=lids_table", "--venaTable"});
		
		ETLMetadataDTO metadata = Main.buildETLMetadata(args, etlClient);
		
		assertEquals(modelId, metadata.getModelId());
		assertEquals("Loading LIDs file", metadata.getName());
		assertEquals(1, metadata.getSteps().size());
		
		ETLStepDTO step = metadata.getSteps().get(0);
		
		assertEquals(ETLFileToRedshiftStepDTO.class, step.getClass());

		ETLFileToRedshiftStepDTO fileToRedshiftStep = (ETLFileToRedshiftStepDTO) step;
		assertEquals(DataType.lids, fileToRedshiftStep.getDataType());
		assertEquals("lidsFile.csv", fileToRedshiftStep.getFileName());
		assertEquals("lids_table", fileToRedshiftStep.getTableName());
		assertEquals(FileFormat.CSV, fileToRedshiftStep.getFileFormat());
	}
	
	@Test
	public void testFileToRedshiftWithEncoding() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Loading LIDs file", "--file", "lidsFile.csv;type=lids;format=CSV;table=lids_table;encoding=ASCII", "--venaTable"});

		ETLMetadataDTO metadata = Main.buildETLMetadata(args, etlClient);

		assertEquals(modelId, metadata.getModelId());
		assertEquals("Loading LIDs file", metadata.getName());
		assertEquals(1, metadata.getSteps().size());

		ETLStepDTO step = metadata.getSteps().get(0);

		assertEquals(ETLFileToRedshiftStepDTO.class, step.getClass());

		ETLFileToRedshiftStepDTO fileToStageStep = (ETLFileToRedshiftStepDTO) step;
		assertEquals(DataType.lids, fileToStageStep.getDataType());
		assertEquals("lidsFile.csv", fileToStageStep.getFileName());
		assertEquals("lids_table", fileToStageStep.getTableName());
		assertEquals(FileFormat.CSV, fileToStageStep.getFileFormat());
		assertEquals("ASCII", fileToStageStep.getFileEncoding());
	}
}
