package org.vena.etltool.tests;

import static org.junit.Assert.assertEquals;

import java.io.UnsupportedEncodingException;

import org.junit.Test;
import org.vena.etltool.ETLClient;
import org.vena.etltool.Main;
import org.vena.etltool.entities.ETLMetadataDTO;
import org.vena.etltool.entities.ETLStepDTO;
import org.vena.etltool.entities.ETLStreamChannelStepDTO;
import org.vena.etltool.entities.ETLStreamStepDTO.MockMode;
import org.vena.etltool.entities.Id;

public class ETLToolRunChannelTest extends ETLToolTest {

	@Test
	public void testRunChannel() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Running integrations channel", "--runChannel", "1234"});

		ETLMetadataDTO metadata = Main.buildETLMetadata(args, etlClient);

		assertEquals(modelId, metadata.getModelId());
		assertEquals("Running integrations channel", metadata.getName());
		assertEquals(1, metadata.getSteps().size());
		
		ETLStepDTO step = metadata.getSteps().get(0);

		assertEquals(ETLStreamChannelStepDTO.class, step.getClass());

		ETLStreamChannelStepDTO integrationsStep = (ETLStreamChannelStepDTO)step;
		assertEquals(new Id(1234), integrationsStep.getSourceId());
		assertEquals(MockMode.LIVE, integrationsStep.getMockMode());
	}

	@Test
	public void testMultipleRunChannel() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Running integrations channels", "--runChannel", "1234", "--runChannel", "5678"});

		ETLMetadataDTO metadata = Main.buildETLMetadata(args, etlClient);

		assertEquals(modelId, metadata.getModelId());
		assertEquals("Running integrations channels", metadata.getName());
		assertEquals(2, metadata.getSteps().size());

		ETLStepDTO firstStep = metadata.getSteps().get(0);
		ETLStepDTO secondStep = metadata.getSteps().get(1);

		assertEquals(ETLStreamChannelStepDTO.class, firstStep.getClass());
		assertEquals(ETLStreamChannelStepDTO.class, secondStep.getClass());

		ETLStreamChannelStepDTO firstIntegrationsStep = (ETLStreamChannelStepDTO)firstStep;
		ETLStreamChannelStepDTO secondIntegrationsStep = (ETLStreamChannelStepDTO)secondStep;

		assertEquals(new Id(1234), firstIntegrationsStep.getSourceId());
		assertEquals(MockMode.LIVE, firstIntegrationsStep.getMockMode());

		assertEquals(new Id(5678), secondIntegrationsStep.getSourceId());
		assertEquals(MockMode.LIVE, secondIntegrationsStep.getMockMode());
	}

	@Test
	public void testRunChannelMissingArg() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Running integrations channels", "--runChannel"});

		try {
			Main.buildETLMetadata(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: Missing argument for option: runChannel", err.toString().trim());
		}
	}

	@Test
	public void testMultipleRunChannelMissingArg() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Running integrations channels", "--runChannel", "--runChannel", "5678"});

		try {
			Main.buildETLMetadata(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: Missing argument for option: runChannel", err.toString().trim());
		}
	}

	@Test
	public void testRunChannelInvalidId() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--jobName", "Running integrations channels", "--runChannel", "1234a", "--runChannel", "5678"});

		try {
			Main.buildETLMetadata(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: channelId could not be parsed as a number.", err.toString().trim());
		}
	}
}