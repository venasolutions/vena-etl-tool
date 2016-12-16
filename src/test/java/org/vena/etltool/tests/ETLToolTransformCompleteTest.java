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
		ETLClient etlClient = buildETLClient();
		Id jobId = uniqueId();
		String[] args = buildCommand(new String[] {"--transformComplete", "--jobId", jobId.toString()});
		
		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			verify(etlClient).sendTransformComplete(jobId.toString());
			assertEquals(0, e.status);
		}
	}
	
	@Test
	public void testTransformCompleteMissingJobId() throws UnsupportedEncodingException {
		ETLClient etlClient = buildETLClient();
		String[] args = buildCommand(new String[] {"--transformComplete"});
		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: You must specify --jobId=<job Id>.\n", err.toString());
		}
	}
}
