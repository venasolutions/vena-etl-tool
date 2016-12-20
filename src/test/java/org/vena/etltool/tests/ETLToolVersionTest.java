package org.vena.etltool.tests;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

import java.io.UnsupportedEncodingException;

import org.junit.Test;
import org.vena.etltool.ETLClient;
import org.vena.etltool.Main;

public class ETLToolVersionTest extends ETLToolTest {
	
	@SuppressWarnings("static-access")
	@Test
	public void testRequestVersion() throws UnsupportedEncodingException {
		ETLClient etlClient = buildETLClient();
		String[] args = new String[] {"--version"};
		
		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			verify(etlClient).requestVersionInfo();
			assertEquals(0, e.status);
		}
	}

}
