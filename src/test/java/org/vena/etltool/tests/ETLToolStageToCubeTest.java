package org.vena.etltool.tests;

import static org.junit.Assert.assertEquals;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.vena.etltool.ETLClient;
import org.vena.etltool.Main;
import org.vena.etltool.entities.ETLMetadataDTO;
import org.vena.etltool.entities.ETLStageToCubeStepDTO;
import org.vena.etltool.entities.ETLStepDTO;
import org.vena.etltool.entities.ETLStepDTO.DataType;

public class ETLToolStageToCubeTest extends ETLToolTest {
	
	@Test
	public void testStageToCube() throws UnsupportedEncodingException {
		ETLClient etlClient = buildETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Loading to stage", "--loadFromStaging"});
		
		ETLMetadataDTO metadata = Main.parseCmdlineArgs(args, etlClient);
		
		assertEquals(modelId, metadata.getModelId());
		assertEquals("Loading to stage", metadata.getName());
		assertEquals(4, metadata.getSteps().size());
		
		List<ETLStepDTO> steps = metadata.getSteps();
		List<DataType> types = Arrays.asList(DataType.hierarchy, DataType.attributes, DataType.intersections, DataType.lids);
		
		for (int i=0; i<steps.size(); i++) { 
			assertEquals(ETLStageToCubeStepDTO.class, steps.get(i).getClass());
			assertEquals(types.get(i), ((ETLStageToCubeStepDTO)steps.get(i)).getDataType());
		}
	}
	
	@Test
	public void testStageToCubeWithClear() throws UnsupportedEncodingException {
		ETLClient etlClient = buildETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Loading to stage with clear", "--loadFromStaging", "--clearSlices", "dimension('Accounts':'Expense'),dimension('Accounts':'Sale')"});
		
		ETLMetadataDTO metadata = Main.parseCmdlineArgs(args, etlClient);
		
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
	public void testMultipleStageOperations() throws UnsupportedEncodingException {
		ETLClient etlClient = buildETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Hello world", "--loadFromStaging", "--stageOnly"});
		
		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: --stage, --stageAndTransform, --stageOnly, and --loadFromStaging options cannot be combined. At most one of these options can be used at a time.\n", err.toString());
		}
	}
}
