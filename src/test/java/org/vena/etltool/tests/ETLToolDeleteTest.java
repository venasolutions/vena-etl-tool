package org.vena.etltool.tests;

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;

import org.apache.commons.lang3.ArrayUtils;
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
	
	ETLClient etlClient = buildETLClient();

	@Test
	public void testDeleteLids() throws UnsupportedEncodingException {
		String[] args = buildCommand(new String[] {"--delete", "lids", "--deleteQuery", "dimension('Accounts': 'Expense')", "--jobName", "delete lids for member Expense"});
		
		ETLMetadataDTO metadata = Main.parseCmdlineArgs(args, etlClient);
		assertEquals(metadata.getModelId(), modelId);
		assertEquals(metadata.getName(), "delete lids for member Expense");
		assertEquals(metadata.getSteps().size(), 1);
		
		ETLStepDTO step = metadata.getSteps().get(0);
		
		assertEquals(step.getClass(), ETLDeleteLidsStepDTO.class);
		assertEquals(((ETLDeleteLidsStepDTO)step).getDataType(), DataType.lids);
		assertEquals(((ETLDeleteLidsStepDTO)step).getExpression(), "dimension('Accounts': 'Expense')");
	}
	
	@Test
	public void testDeleteValues() throws UnsupportedEncodingException {
		String[] deleteArgs = {"--delete", "values", "--deleteQuery", "dimension('Accounts': 'Expense')", "--jobName", "delete values for member Expense"};
		String[] args = ArrayUtils.addAll(buildCommandArgs(), deleteArgs);
		
		ETLMetadataDTO metadata = Main.parseCmdlineArgs(args, etlClient);
		assertEquals(metadata.getModelId(), modelId);
		assertEquals(metadata.getName(), "delete values for member Expense");
		assertEquals(metadata.getSteps().size(), 1);
		
		ETLStepDTO step = metadata.getSteps().get(0);
		
		assertEquals(step.getClass(), ETLDeleteValuesStepDTO.class);
		assertEquals(((ETLDeleteValuesStepDTO)step).getDataType(), DataType.values);
		assertEquals(((ETLDeleteValuesStepDTO)step).getExpression(), "dimension('Accounts': 'Expense')");
	}
	
	@Test
	public void testDeleteIntersections() throws UnsupportedEncodingException {
		String[] deleteArgs = {"--delete", "intersections", "--deleteQuery", "dimension('Accounts': 'Expense')", "--jobName", "delete intersections for member Expense"};
		String[] args = ArrayUtils.addAll(buildCommandArgs(), deleteArgs);
		
		ETLMetadataDTO metadata = Main.parseCmdlineArgs(args, etlClient);
		assertEquals(metadata.getModelId(), modelId);
		assertEquals(metadata.getName(), "delete intersections for member Expense");
		assertEquals(metadata.getSteps().size(), 1);
		
		ETLStepDTO step = metadata.getSteps().get(0);
		
		assertEquals(step.getClass(), ETLDeleteIntersectionsStepDTO.class);
		assertEquals(((ETLDeleteIntersectionsStepDTO)step).getDataType(), DataType.intersections);
		assertEquals(((ETLDeleteIntersectionsStepDTO)step).getExpression(), "dimension('Accounts': 'Expense')");
	}
}
