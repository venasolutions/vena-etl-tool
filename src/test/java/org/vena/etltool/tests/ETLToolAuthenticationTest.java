package org.vena.etltool.tests;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

import org.junit.*;
import org.vena.etltool.ETLClient;
import org.vena.etltool.Main;

public class ETLToolAuthenticationTest extends ETLToolTest {
	
	private PrintStream originalSystemErr = System.err;
	private PrintStream originalSystemOut = System.out;
	
	private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
	private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();

	@Before
	public void setUpStreams() {
	    System.setOut(new PrintStream(outContent));
	    System.setErr(new PrintStream(errContent));
	}

	@After
	public void cleanUpStreams() {
	    System.setOut(originalSystemErr);
	    System.setErr(originalSystemOut);
	}
	
	@Test
	public void testUsernameAndPwd() throws UnsupportedEncodingException {
		ETLClient etlClient = buildETLClient();
		setNoExitSecurityManager();
		try {
			Main.parseCmdlineArgs(buildCommandArgs(), etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: You must specify at least one --file option when submitting a job.\n", errContent.toString());
			assertEquals("vena", etlClient.password);
			assertEquals("admin@vena.io", etlClient.username);
		}
	}
	
	
}
