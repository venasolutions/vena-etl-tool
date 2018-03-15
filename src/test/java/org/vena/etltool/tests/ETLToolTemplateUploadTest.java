package org.vena.etltool.tests;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.junit.Test;
import org.vena.etltool.ETLClient;
import org.vena.etltool.Main;
import org.vena.etltool.entities.ETLDeleteDimensionStepDTO;
import org.vena.etltool.entities.ETLFileToCubeStepDTO;
import org.vena.etltool.entities.ETLFileToStageStepDTO;
import org.vena.etltool.entities.ETLMetadataDTO;
import org.vena.etltool.entities.ETLStepDTO;
import org.vena.etltool.entities.ETLStepDTO.DataType;
import org.vena.etltool.entities.ETLTemplateDTO;
import org.vena.etltool.entities.ETLVersioningClearStepDTO;
import org.vena.etltool.entities.Id;

public class ETLToolTemplateUploadTest extends ETLToolTest {

	@Test
	public void testGetTemplateMetadataWithFiles() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		when(etlClient.getETLTemplate()).thenReturn(produceFileTemplate());
		CommandLine commandLine = Main.parseCommandLineArgs(buildCommand(new String[] { "--runTemplate=12345",
				"--file=file1.csv;type=hierarchy", "--file=file2.csv;type=intersections" }));
		ETLMetadataDTO metadata = Main.produceETLMetadata(etlClient, commandLine);
		validateFileTemplateMetadata(metadata);
	}

	@Test
	public void testGetTemplateMetadataWithoutEnoughFiles() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		when(etlClient.getETLTemplate()).thenReturn(produceFileTemplate());
		CommandLine commandLine = Main.parseCommandLineArgs(
				buildCommand(new String[] { "--runTemplate=12345", "--file=file.csv;type=hierarchy", }));
		try {
			Main.produceETLMetadata(etlClient, commandLine);
			fail();
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertTrue(err.toString().contains("Please provide the correct number of input files to run template"));
		}
	}

	@Test
	public void testGetTemplateMetadataWrongFileFormat() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		when(etlClient.getETLTemplate()).thenReturn(produceFileTemplate());
		CommandLine commandLine = Main.parseCommandLineArgs(buildCommand(new String[] { "--runTemplate=12345",
				"--file=file2.csv;type=intersections", "--file=file.csv;type=hierarchy" }));
		try {
			Main.produceETLMetadata(etlClient, commandLine);
			fail();
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertTrue(err.toString().contains("File step type must match input file type"));
		}
	}

	@Test
	public void testGetTemplateMetadataTooManyFiles() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		when(etlClient.getETLTemplate()).thenReturn(produceFileTemplate());
		CommandLine commandLine = Main.parseCommandLineArgs(
				buildCommand(new String[] { "--runTemplate=12345", "--file=file1.csv;type=hierarchy",
						"--file=file2.csv;type=intersections", "--file=file3.csv;type=lids" }));
		try {
			Main.produceETLMetadata(etlClient, commandLine);
			fail();
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertTrue(err.toString().contains("Please provide the correct number of input files to run template"));
		}
	}

	@Test
	public void testGetTemplateMetadata() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		when(etlClient.getETLTemplate()).thenReturn(produceNoFileTemplate());
		CommandLine commandLine = Main.parseCommandLineArgs(buildCommand(new String[] { "--runTemplate=12345" }));
		ETLMetadataDTO metadata = Main.produceETLMetadata(etlClient, commandLine);
		assertEquals("ETL JOB", metadata.getName());
	}

	@Test
	public void testGetTemplateMetadataError() throws UnsupportedEncodingException {
		try {
			Main.parseCommandLineArgs(buildCommand(new String[] { "--runTemplate" }));
			fail();
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertTrue(err.toString().contains("Error"));
		}
	}

	public void validateFileTemplateMetadata(ETLMetadataDTO metadata) {
		assertEquals("ETL JOB WITH FILES", metadata.getName());
		ETLStepDTO step1 = metadata.getSteps().get(0);
		if (step1 instanceof ETLFileToCubeStepDTO) {
			ETLFileToCubeStepDTO fileStep1 = (ETLFileToCubeStepDTO) step1;
			assertEquals("file1.csv", fileStep1.getFileName());
			assertEquals(DataType.hierarchy, fileStep1.getDataType());
		} else {
			fail("Incorrect step type.");
		}

		ETLStepDTO step2 = metadata.getSteps().get(1);
		if (step2 instanceof ETLFileToStageStepDTO) {
			ETLFileToStageStepDTO fileStep2 = (ETLFileToStageStepDTO) step2;
			assertEquals("file2.csv", fileStep2.getFileName());
			assertEquals(DataType.intersections, fileStep2.getDataType());
		} else {
			fail("Incorrect step type.");
		}
	}

	public ETLTemplateDTO produceNoFileTemplate() {
		ETLTemplateDTO template = new ETLTemplateDTO();
		template.setId(new Id(12345L));
		ETLMetadataDTO metadata = new ETLMetadataDTO();
		metadata.setName("ETL JOB");
		ETLDeleteDimensionStepDTO step1 = new ETLDeleteDimensionStepDTO(2);
		ETLVersioningClearStepDTO step2 = new ETLVersioningClearStepDTO();
		List<ETLStepDTO> steps = new ArrayList<>();
		steps.add(step1);
		steps.add(step2);
		metadata.setSteps(steps);
		template.setMetadata(metadata);
		return template;
	}

	public ETLTemplateDTO produceFileTemplate() {
		ETLTemplateDTO template = new ETLTemplateDTO();
		template.setId(new Id(12345L));
		ETLMetadataDTO metadata = new ETLMetadataDTO();
		metadata.setName("ETL JOB WITH FILES");
		ETLFileToCubeStepDTO step1 = new ETLFileToCubeStepDTO();
		step1.setDataType(DataType.hierarchy);
		ETLFileToStageStepDTO step2 = new ETLFileToStageStepDTO();
		step2.setDataType(DataType.intersections);
		List<ETLStepDTO> steps = new ArrayList<>();
		steps.add(step1);
		steps.add(step2);
		metadata.setSteps(steps);
		template.setMetadata(metadata);
		return template;
	}
}