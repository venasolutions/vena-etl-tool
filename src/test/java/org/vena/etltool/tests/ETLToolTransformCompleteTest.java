package org.vena.etltool.tests;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.io.UnsupportedEncodingException;

import org.junit.Test;
import org.vena.etltool.ETLClient;
import org.vena.etltool.Main;
import org.vena.etltool.entities.Id;

public class ETLToolTransformCompleteTest extends ETLToolTest {
	
	@Test
	public void testTransformComplete() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		Id jobId = uniqueId();
		String[] args = buildCommand(new String[] {"--transformComplete", "--jobId", jobId.toString()});
		
		try {
			Main.buildETLMetadata(args, etlClient);
		} catch (ExitException e) {
			verify(etlClient).sendTransformComplete(jobId.toString().trim());
			assertEquals(0, e.status);
		}
	}
	
	@Test
	public void testTransformCompleteMissingJobId() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--transformComplete"});
		try {
			Main.buildETLMetadata(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: You must specify --jobId=<job Id>.", err.toString().trim());
		}
	}
}
