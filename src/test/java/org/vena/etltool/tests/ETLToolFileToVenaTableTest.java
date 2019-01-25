package org.vena.etltool.tests;

import org.junit.Test;
import org.vena.etltool.ETLClient;
import org.vena.etltool.Main;
import org.vena.etltool.entities.ETLFileImportStepDTO.FileFormat;
import org.vena.etltool.entities.ETLFileToVenaTableStepDTO;
import org.vena.etltool.entities.ETLMetadataDTO;
import org.vena.etltool.entities.ETLStepDTO;
import org.vena.etltool.entities.ETLStepDTO.DataType;

import java.io.UnsupportedEncodingException;

import static org.junit.Assert.assertEquals;

public class ETLToolFileToVenaTableTest extends ETLToolTest {

	@Test
	public void testFileToVenaTable() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Loading LIDs file", "--file", "lidsFile.csv;type=lids;format=CSV;table=lids_table", "--venaTable"});
		
		ETLMetadataDTO metadata = Main.buildETLMetadata(args, etlClient);
		
		assertEquals(modelId, metadata.getModelId());
		assertEquals("Loading LIDs file", metadata.getName());
		assertEquals(1, metadata.getSteps().size());
		
		ETLStepDTO step = metadata.getSteps().get(0);
		
		assertEquals(ETLFileToVenaTableStepDTO.class, step.getClass());

		ETLFileToVenaTableStepDTO fileToVenaTableStep = (ETLFileToVenaTableStepDTO) step;
		assertEquals(DataType.lids, fileToVenaTableStep.getDataType());
		assertEquals("lidsFile.csv", fileToVenaTableStep.getFileName());
		assertEquals("lids_table", fileToVenaTableStep.getTableName());
		assertEquals(FileFormat.CSV, fileToVenaTableStep.getFileFormat());
	}

	@Test
	public void testFileToVenaTableWithEncodingWithoutType() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Loading LIDs file", "--file", "lidsFile.csv;format=CSV;table=lids_table;encoding=ASCII", "--venaTable"});

		ETLMetadataDTO metadata = Main.buildETLMetadata(args, etlClient);

		assertEquals(modelId, metadata.getModelId());
		assertEquals("Loading LIDs file", metadata.getName());
		assertEquals(1, metadata.getSteps().size());

		ETLStepDTO step = metadata.getSteps().get(0);

		assertEquals(ETLFileToVenaTableStepDTO.class, step.getClass());

		ETLFileToVenaTableStepDTO fileToStageStep = (ETLFileToVenaTableStepDTO) step;
		assertEquals("lidsFile.csv", fileToStageStep.getFileName());
		assertEquals("lids_table", fileToStageStep.getTableName());
		assertEquals(FileFormat.CSV, fileToStageStep.getFileFormat());
		assertEquals("ASCII", fileToStageStep.getFileEncoding());
	}

	@Test
	public void testSetErrorWithoutTable() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Loading LIDs file", "--file", "lidsFile.csv;format=CSV;encoding=ASCII", "--venaTable"});
		
		try {
			Main.buildETLMetadata(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: The table name must be specified for the Vena Table step.", err.toString().trim());
		}
	}
}
