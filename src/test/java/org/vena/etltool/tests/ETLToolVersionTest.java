package org.vena.etltool.tests;

import static org.junit.Assert.assertEquals;

import java.io.UnsupportedEncodingException;

import org.junit.Test;
import org.vena.etltool.ETLClient;
import org.vena.etltool.Main;

public class ETLToolVersionTest extends ETLToolTest {
	
	@Test
	public void testRequestVersion() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = new String[] {"--version"};
		
		try {
			Main.buildETLMetadata(args, etlClient);
		} catch (ExitException e) {
			assertEquals(0, e.status);
		}
	}

}
