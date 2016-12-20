package org.vena.etltool.tests;

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;

import org.junit.Test;
import org.vena.etltool.ETLClient;
import org.vena.etltool.Main;
import org.vena.etltool.entities.ETLDeleteIntersectionsStepDTO;
import org.vena.etltool.entities.ETLDeleteLidsStepDTO;
import org.vena.etltool.entities.ETLDeleteValuesStepDTO;
import org.vena.etltool.entities.ETLMetadataDTO;
import org.vena.etltool.entities.ETLStepDTO;
import org.vena.etltool.entities.ETLStepDTO.DataType;

public class ETLToolDeleteTest extends ETLToolTest {

	@Test
	public void testDeleteLids() throws UnsupportedEncodingException {
		ETLClient etlClient = buildETLClient();
		String[] args = buildCommand(new String[] {"--delete", "lids", "--deleteQuery", "dimension('Accounts': 'Expense')"});
		
		ETLMetadataDTO metadata = Main.parseCmdlineArgs(args, etlClient);
		assertEquals(metadata.getModelId(), modelId);
		assertEquals(metadata.getSteps().size(), 1);
		
		ETLStepDTO step = metadata.getSteps().get(0);
		
		assertEquals(true, etlClient.pollingRequested);
		assertEquals(true, etlClient.waitFully);
		assertEquals(ETLDeleteLidsStepDTO.class, step.getClass());
		
		ETLDeleteLidsStepDTO deleteLidsStep = (ETLDeleteLidsStepDTO)step;
		assertEquals(DataType.lids, deleteLidsStep.getDataType());
		assertEquals("dimension('Accounts': 'Expense')", deleteLidsStep.getExpression());
	}
	
	@Test
	public void testDeleteValues() throws UnsupportedEncodingException {
		ETLClient etlClient = buildETLClient();
		String[] args = buildCommand(new String[] {"--delete", "values", "--deleteQuery", "dimension('Accounts': 'Expense')"});
		
		ETLMetadataDTO metadata = Main.parseCmdlineArgs(args, etlClient);
		assertEquals(modelId, metadata.getModelId());
		assertEquals(1, metadata.getSteps().size());
		
		ETLStepDTO step = metadata.getSteps().get(0);
		
		assertEquals(true, etlClient.pollingRequested);
		assertEquals(true, etlClient.waitFully);
		assertEquals(ETLDeleteValuesStepDTO.class, step.getClass());
		
		ETLDeleteValuesStepDTO deleteValuesStep = (ETLDeleteValuesStepDTO)step;
		assertEquals(DataType.values, deleteValuesStep.getDataType());
		assertEquals("dimension('Accounts': 'Expense')", deleteValuesStep.getExpression());
	}
	
	@Test
	public void testDeleteIntersections() throws UnsupportedEncodingException {
		ETLClient etlClient = buildETLClient();
		String[] args = buildCommand(new String[] {"--delete", "intersections", "--deleteQuery", "dimension('Accounts': 'Expense')"});
		
		ETLMetadataDTO metadata = Main.parseCmdlineArgs(args, etlClient);
		assertEquals(metadata.getModelId(), modelId);
		assertEquals(metadata.getSteps().size(), 1);
		
		ETLStepDTO step = metadata.getSteps().get(0);
		
		assertEquals(true, etlClient.pollingRequested);
		assertEquals(true, etlClient.waitFully);
		assertEquals(ETLDeleteIntersectionsStepDTO.class, step.getClass());
		
		ETLDeleteIntersectionsStepDTO deleteIntStep = (ETLDeleteIntersectionsStepDTO)step;
		assertEquals(DataType.intersections, deleteIntStep.getDataType());
		assertEquals("dimension('Accounts': 'Expense')", deleteIntStep.getExpression());
	}
	
	@Test
	public void testDeleteNoQuery() throws UnsupportedEncodingException {
		ETLClient etlClient = buildETLClient();
		String[] args = buildCommand(new String[] {"--delete", "intersections"});
		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: delete option requires --deleteQuery <expr>.\n", err.toString());
		}
	}
	
	@Test
	public void testDeleteMissingDeleteQuery() throws UnsupportedEncodingException {
		ETLClient etlClient = buildETLClient();
		String[] args = buildCommand(new String[] {"--delete", "--intersections", "--deleteQuery"});
		
		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: Missing argument for option: deleteQuery\n", err.toString());
		}
	}
	
	@Test
	public void testInvalidDataType() throws UnsupportedEncodingException {
		ETLClient etlClient = buildETLClient();
		String[] args = buildCommand(new String[] {"--delete", "invalid", "--deleteQuery", "dimension('Accounts': 'Expense')"});
		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: The ETL file type \"invalid\" is not supported. The supported filetypes are intersections, values, and lids.\n", err.toString());
		}
	}
	
}
