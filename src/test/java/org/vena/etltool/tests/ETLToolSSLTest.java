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
		ETLClient etlClient = mockETLClient();
		try {
			Main.buildETLMetadata(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: You must specify at least one --file option when submitting a job.", err.toString().trim());
			assertEquals("https", etlClient.protocol);
		}
	}
	
	@Test
	public void testNoSsl() throws UnsupportedEncodingException {
		String[] args = buildCommand(new String[] {"--nossl"});
		ETLClient etlClient = mockETLClient();
		try {
			Main.buildETLMetadata(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: You must specify at least one --file option when submitting a job.", err.toString().trim());
			assertEquals("http", etlClient.protocol);
		}
	}
	
	@Test
	public void testErrBothOptions() throws UnsupportedEncodingException {
		String[] args = buildCommand(new String[] {"--nossl", "--ssl"});
		ETLClient etlClient = mockETLClient();
		try {
			Main.buildETLMetadata(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: --ssl and --nossl options cannot be combined.", err.toString().trim());
		}
	}

}
