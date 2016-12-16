package org.vena.etltool.tests;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

import java.io.UnsupportedEncodingException;

import org.junit.Test;
import org.vena.etltool.ETLClient;
import org.vena.etltool.Main;
import org.vena.etltool.entities.Id;

public class ETLToolCancelTest extends ETLToolTest {

	@Test
	public void testCancelJob() throws UnsupportedEncodingException {
		ETLClient etlClient = buildETLClient();
		Id jobId = uniqueId();
		String[] args = buildCommand(new String[] {"--cancel", "--jobId", jobId.toString()});
		
		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			verify(etlClient).sendCancel(jobId.toString());
			assertEquals(0, e.status);
		}
	}
	
	@Test
	public void testCancelMissingJobId() throws UnsupportedEncodingException {
		ETLClient etlClient = buildETLClient();
		String[] args = buildCommand(new String[] {"--cancel"});
		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: You must specify --jobId=<job Id>.\n", err.toString());
		}
	}
}
