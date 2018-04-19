package org.vena.etltool.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.vena.etltool.ETLClient;
import org.vena.etltool.Main;
import org.vena.etltool.entities.ETLCubeToStageStepDTO;
import org.vena.etltool.entities.ETLCubeToStageStepDTO.QueryType;
import org.vena.etltool.entities.ETLFileImportStepDTO.FileFormat;
import org.vena.etltool.entities.ETLMetadataDTO;
import org.vena.etltool.entities.ETLStepDTO;
import org.vena.etltool.entities.ETLStepDTO.DataType;

public class ETLToolExportTest extends ETLToolTest {

	ETLClient etlClient = mockETLClient();

	protected String[] buildExportToFileCommand(String[] cmdArgs, File outFile) {
		String[] authArgs = buildCommandArgs();
		String[] args = Arrays.copyOf(authArgs, authArgs.length + cmdArgs.length + 2);

		int j = authArgs.length;
		for (int i=0; i<cmdArgs.length; i++) {
			args[j++] = cmdArgs[i];
		}

		args[j++] = "--exportToFile";
		args[j++] = outFile.getPath();

		return args;
	}

	private void testExportToFileSuccess(String[] args) throws IOException {
		InputStream intersection_data = getClass().getClassLoader().getResourceAsStream("exportIntersectionsSource.csv");
		when(etlClient.sendExport(any(DataType.class), any(String.class), any(String.class), any(String.class), any(String.class), anyBoolean(), any(FileFormat.class))).thenReturn(intersection_data);

		File file = File.createTempFile("exportIntersectionsDestination", ".csv");
		file.deleteOnExit();

		args = buildExportToFileCommand(args, file);

		try {
			Main.buildETLMetadata(args, etlClient);
		} catch (ExitException e) {
			assertEquals(0, e.status);
			assertEquals(true, etlClient.pollingRequested);
			assertEquals(true, etlClient.waitFully);
			
			// Check that the contents of the destination file are correct
			String sourcePath = "src" + File.separatorChar + "test" + File.separatorChar + "resources" + File.separatorChar + "exportIntersectionsSource.csv";
			List<String> source = Files.readAllLines(Paths.get(sourcePath));
			List<String> destination = Files.readAllLines(file.toPath());
			assertEquals(source, destination);
			return;
		}
		fail("Did not exit.");
	}

	@Test
	public void testExportToFile() throws IOException {
		testExportToFileSuccess(new String[] {"--export","intersections"}); // no format, should default to CSV

		verify(etlClient).sendExport(DataType.intersections, null, null, null, null, true, FileFormat.CSV);
	}

	@Test
	public void testExportToFileCSV() throws IOException {
		testExportToFileSuccess(new String[] {"--export","lids","--exportFormat","CSV"});

		verify(etlClient).sendExport(DataType.lids, null, null, null, null, true, FileFormat.CSV);
	}

	@Test
	public void testExportToFileTDF() throws IOException {
		testExportToFileSuccess(new String[] {"--export","attributes","--exportFormat","TDF"});

		verify(etlClient).sendExport(DataType.attributes, null, null, null, null, true, FileFormat.TDF);
	}

	@Test
	public void testExportToFilePSV() throws IOException {
		testExportToFileSuccess(new String[] {"--export","hierarchy","--exportFormat","PSV"});

		verify(etlClient).sendExport(DataType.hierarchy, null, null, null, null, true, FileFormat.PSV);
	}

	@Test
	public void testExportFromTableToFile() throws IOException {
		testExportToFileSuccess(new String[] {"--export","staging","--exportFromTable","out_values"});

		verify(etlClient).sendExport(DataType.user_defined, "out_values", null, null, null, true, FileFormat.CSV);
	}

	@Test
	public void testExportToTable() throws UnsupportedEncodingException {
		String[] args = buildCommand(new String[] {"--jobName","Stage to cube job","--export","intersections","--exportToTable", "export_table","--exportQuery","dimension('Accounts':'Sale')"});
		
		ETLMetadataDTO metadata = Main.buildETLMetadata(args, etlClient);
		
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
		String[] args = buildCommand(new String[] {"--export","invalidType", "--exportToFile", "intersectionsFile.csv"});
		
		try {
			Main.buildETLMetadata(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: The ETL file type \"invalidType\" does not exist. The known filetypes are "
			+ "[intersections, values, lids, hierarchy, dimensions, attributes, user_defined, "
			+ "intersection_members, lid_members, variables, setexpressions]", err.toString().trim());
		}
	}

	@Test
	public void testInvalidExportFormat() throws UnsupportedEncodingException {
		String[] args = buildCommand(new String[] {"--export","intersections", "--exportToFile", "intersectionsFile.csv", "--exportFormat", "invalid"});
		
		try {
			Main.buildETLMetadata(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: The export file format \"invalid\" does not exist. The known formats are [CSV, PSV, TDF]", err.toString().trim());
		}
	}

	@Test
	public void testExportQueryWithExportWhere() throws UnsupportedEncodingException {
		String[] args = buildCommand(new String[] {"--export","intersections","--exportQuery","query", "--exportWhere","whereQueyr","--exportToFile","intersectionsFile.csv"});
		
		try {
			Main.buildETLMetadata(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: exportWhere and exportQuery options cannot be combined.", err.toString().trim());
		}
	}
	
	@Test
	public void testExportQueryWithExportFromTable() throws UnsupportedEncodingException {
		String[] args = buildCommand(new String[] {"--exportFromTable","intersections_table","--exportQuery","query", "--exportToFile","intersectionsFile.csv"});
		
		try {
			Main.buildETLMetadata(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: cannot use --exportQuery with --exportFromTable. Use --exportWhere \"<HQL Query>\" instead.", err.toString().trim());
		}
	}
	
	@Test
	public void testExportToTableFromTable() throws UnsupportedEncodingException {
		String[] args = buildCommand(new String[] {"--exportFromTable","intersections_table","--exportToTable","out_table"});
		
		try {
			Main.buildETLMetadata(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: cannot export from table to another table.", err.toString().trim());
		}
	}
	
	@Test
	public void testExportMissingDestination() throws UnsupportedEncodingException {
		String[] args = buildCommand(new String[] {"--export","intersections"});
		
		try {
			Main.buildETLMetadata(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals(true, etlClient.pollingRequested);
			assertEquals(true, etlClient.waitFully);
			assertEquals("Error: export option requires either --exportToTable <name> or --exportToFile <name>.", err.toString().trim());
		}
	}
	
	@Test
	public void testExportAndExportFromTable() throws UnsupportedEncodingException {
		String[] args = buildCommand(new String[] {"--export","intersections","--exportFromTable", "values_table"});
		
		try {
			Main.buildETLMetadata(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals(true, etlClient.pollingRequested);
			assertEquals(true, etlClient.waitFully);
			assertEquals("Error: --export <intersections> is not supported with --exportFromTable", err.toString().trim());
		}
	}
	
	@Test
	public void testExportToError() throws UnsupportedEncodingException {
		String[] args = buildCommand(new String[] {"--export","intersections","--exportToFile", "intersectionsFile.csv", "--exportToTable", "values_table"});
		
		try {
			Main.buildETLMetadata(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals(true, etlClient.pollingRequested);
			assertEquals(true, etlClient.waitFully);
			assertEquals("Error: --exportToTable and --exportToFile options cannot be combined.", err.toString().trim());
		}
	}
}
