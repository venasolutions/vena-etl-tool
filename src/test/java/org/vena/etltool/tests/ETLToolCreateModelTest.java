package org.vena.etltool.tests;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.io.UnsupportedEncodingException;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.vena.etltool.ETLClient;
import org.vena.etltool.Main;
import org.vena.etltool.entities.Id;

public class ETLToolCreateModelTest extends ETLToolTest {
	
	private ETLClient etlClient;
	Id newModelId;
	
	@Test
	public void testCreateModel() throws UnsupportedEncodingException {
		newModelId = uniqueId();
		etlClient = mockETLClient();
		when(etlClient.createModel("newModel")).then(setModelId);
		String[] args = new String[] {"--username","user@vena.io","--password","Vena123","--createModel","newModel"};
		
		try {
			Main.buildETLMetadata(args, etlClient);
		} catch (ExitException e) {
			verify(etlClient).createModel("newModel");
			assertEquals(1, e.status);
			assertEquals(newModelId, etlClient.modelId);
			assertEquals("Error: You must specify at least one --file option when submitting a job.", err.toString().trim());
		}
	}
	
	@SuppressWarnings("rawtypes")
	Answer setModelId = new Answer() {
		@Override
		public Object answer(InvocationOnMock invocation) {
			etlClient.modelId = newModelId;
			return null;
		};
	};

}
