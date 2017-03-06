package org.vena.etltool.tests;

import static org.junit.Assert.assertEquals;

import java.io.UnsupportedEncodingException;

import org.junit.Test;
import org.vena.etltool.ETLClient;
import org.vena.etltool.Main;

public class ETLToolVerboseTest extends ETLToolTest {
	
	@Test
	public void testVerbose() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--verbose"});
		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: You must specify at least one --file option when submitting a job.\n", err.toString());
			assertEquals(true, etlClient.verbose);
		}
	}
}
