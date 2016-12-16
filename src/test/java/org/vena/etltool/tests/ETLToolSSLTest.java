package org.vena.etltool.tests;

import static org.junit.Assert.assertEquals;

import java.io.UnsupportedEncodingException;

import org.junit.Test;
import org.vena.etltool.ETLClient;
import org.vena.etltool.Main;

public class ETLToolSSLTest extends ETLToolTest {
	
	@Test
	public void testSsl() throws UnsupportedEncodingException {
		String[] args = buildCommand(new String[] {"--ssl"});
		ETLClient etlClient = buildETLClient();
		setNoExitSecurityManager();
		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: You must specify at least one --file option when submitting a job.\n", err.toString());
			assertEquals("https", etlClient.protocol);
		}
	}
	
	@Test
	public void testNoSsl() throws UnsupportedEncodingException {
		String[] args = buildCommand(new String[] {"--nossl"});
		ETLClient etlClient = buildETLClient();
		setNoExitSecurityManager();
		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: You must specify at least one --file option when submitting a job.\n", err.toString());
			assertEquals("http", etlClient.protocol);
		}
	}
	
	@Test
	public void testErrBothOptions() throws UnsupportedEncodingException {
		String[] args = buildCommand(new String[] {"--nossl", "--ssl"});
		ETLClient etlClient = buildETLClient();
		setNoExitSecurityManager();
		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: --ssl and --nossl options cannot be combined.\n", err.toString());
		}
	}

}
