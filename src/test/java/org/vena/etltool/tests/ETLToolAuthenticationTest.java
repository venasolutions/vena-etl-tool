package org.vena.etltool.tests;

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;

import org.junit.*;
import org.vena.etltool.ETLClient;
import org.vena.etltool.Main;

public class ETLToolAuthenticationTest extends ETLToolTest {
	
	@Test
	public void testUsernameAndPwd() throws UnsupportedEncodingException {
		ETLClient etlClient = buildETLClient();
		try {
			Main.parseCmdlineArgs(buildCommandArgs(), etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: You must specify at least one --file option when submitting a job.\n", err.toString());
			assertEquals("vena", etlClient.password);
			assertEquals("admin@vena.io", etlClient.username);
		}
	}
}
