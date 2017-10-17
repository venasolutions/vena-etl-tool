package org.vena.etltool.tests;

import static org.junit.Assert.assertEquals;

import java.io.UnsupportedEncodingException;

import org.junit.Test;
import org.vena.etltool.ETLClient;
import org.vena.etltool.Main;
import org.vena.etltool.entities.Id;

public class ETLToolTemplateIdTest extends ETLToolTest {
	
	@Test
	public void testTemplateId() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		Id templateId = uniqueId();
		String[] args = buildCommand(new String[] {"--templateId", templateId.toString()});
		
		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals(templateId.toString(), etlClient.templateId);
			assertEquals("Error: You must specify at least one --file option when submitting a job.", err.toString().trim());
		}
	}
	
	@Test
	public void testTemplateIdMissingId() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--templateId"});
		
		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: Missing argument for option: templateId", err.toString().trim());
		}
	}

}
