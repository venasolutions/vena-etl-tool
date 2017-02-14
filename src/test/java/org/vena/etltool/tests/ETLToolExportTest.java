package org.vena.etltool.tests;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.junit.Test;
import org.vena.etltool.ETLClient;
import org.vena.etltool.Main;
import org.vena.etltool.entities.ETLCubeToStageStepDTO;
import org.vena.etltool.entities.ETLCubeToStageStepDTO.QueryType;
import org.vena.etltool.entities.ETLMetadataDTO;
import org.vena.etltool.entities.ETLStepDTO;
import org.vena.etltool.entities.ETLStepDTO.DataType;

public class ETLToolExportTest extends ETLToolTest {
	
	@Test
	public void testExportToFile() throws IOException {
		ETLClient etlClient = mockETLClient();
		InputStream intersection_data = getClass().getClassLoader().getResourceAsStream("exportIntersectionsSource.csv");
		when(etlClient.sendExport(DataType.intersections, null, true, null, null, null, true)).thenReturn(intersection_data);
	
		File file = File.createTempFile("exportIntersectionsDestination", ".csv");
		file.deleteOnExit();
		
		String[] args = buildCommand(new String[] {"--export","intersections","--exportToFile", file.getPath()});
		
		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			verify(etlClient).sendExport(DataType.intersections, null, true, null, null, null, true);
			assertEquals(0, e.status);
			assertEquals(true, etlClient.pollingRequested);
			assertEquals(true, etlClient.waitFully);
			
			// Check that the contents of the destination file are correct
			String sourcePath = "src" + File.separatorChar + "test" + File.separatorChar + "resources" + File.separatorChar + "exportIntersectionsSource.csv";
			List<String> source = Files.readAllLines(Paths.get(sourcePath));
			List<String> destination = Files.readAllLines(file.toPath());
			assertEquals(source, destination);
		}
	}
	
	@Test
	public void testExportToTable() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName","Stage to cube job","--export","intersections","--exportToTable", "export_table","--exportQuery","dimension('Accounts':'Sale')"});
		
		ETLMetadataDTO metadata = Main.parseCmdlineArgs(args, etlClient);
		
		assertEquals(modelId, metadata.getModelId());
		assertEquals("Stage to cube job", metadata.getName());
		assertEquals(1, metadata.getSteps().size());
		
		ETLStepDTO step = metadata.getSteps().get(0);
		
		assertEquals(ETLCubeToStageStepDTO.class, step.getClass());
		
		ETLCubeToStageStepDTO cubeToStageStep = (ETLCubeToStageStepDTO) step;
		assertEquals(DataType.intersections, cubeToStageStep.getDataType());
		assertEquals("export_table", cubeToStageStep.getTableName());
		assertEquals(QueryType.MODEL_SLICE, cubeToStageStep.getQueryType());
		assertEquals("dimension('Accounts':'Sale')", cubeToStageStep.getQueryString());

	}
	
	// Tests for expected errors
	
	@Test
	public void testInvalidExportType() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--export","invalidType", "--exportToFile", "intersectionsFile.csv"});
		
		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: The ETL file type \"invalidType\" does not exist. The known filetypes are "
			+ "[intersections, values, lids, hierarchy, dimensions, attributes, user_defined, "
			+ "intersection_members, lid_members, variables, setexpressions, staging]\n", err.toString());
		}
	}
	
	@Test
	public void testExportQueryWithExportWhere() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--export","intersections","--exportQuery","query", "--exportWhere","whereQueyr","--exportToFile","intersectionsFile.csv"});
		
		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: exportWhere and exportQuery options cannot be combined.\n", err.toString());
		}
	}
	
	@Test
	public void testExportQueryWithExportFromTable() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--exportFromTable","intersections_table","--exportQuery","query", "--exportToFile","intersectionsFile.csv"});
		
		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: cannot use --exportQuery with --exportFromTable. Use --exportWhere \"<HQL Query>\" instead. \n", err.toString());
		}
	}
	
	@Test
	public void testExportToTableFromTable() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--exportFromTable","intersections_table","--exportToTable","out_table"});
		
		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: cannot export from table to another table.\n", err.toString());
		}
	}
	
	@Test
	public void testExportMissingDestination() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--export","intersections"});
		
		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals(true, etlClient.pollingRequested);
			assertEquals(true, etlClient.waitFully);
			assertEquals("Error: export option requires either --exportToTable <name> or --exportToFile <name>.\n", err.toString());
		}
	}
	
	@Test
	public void testExportAndExportFromTable() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--export","intersections","--exportFromTable", "values_table"});
		
		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals(true, etlClient.pollingRequested);
			assertEquals(true, etlClient.waitFully);
			assertEquals("Error: --export <intersections> is not supported with --exportFromTable\n", err.toString());
		}
	}
	
	@Test
	public void testExportToError() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--export","intersections","--exportToFile", "intersectionsFile.csv", "--exportToTable", "values_table"});
		
		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals(true, etlClient.pollingRequested);
			assertEquals(true, etlClient.waitFully);
			assertEquals("Error: --exportToTable and --exportToFile options cannot be combined.\n", err.toString());
		}
	}
}
