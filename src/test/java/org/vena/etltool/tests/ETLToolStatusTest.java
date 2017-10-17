package org.vena.etltool.tests;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.io.UnsupportedEncodingException;

import org.junit.Test;
import org.vena.etltool.ETLClient;
import org.vena.etltool.Main;
import org.vena.etltool.entities.ETLJobDTO;
import org.vena.etltool.entities.ETLMetadataDTO;
import org.vena.etltool.entities.ETLStepDTO.Status;

public class ETLToolStatusTest extends ETLToolTest {

	@Test
	public void testStatus() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		ETLJobDTO job = new ETLJobDTO();
		job.setMetadata(new ETLMetadataDTO());
		job.setId(uniqueId());
		job.setStatus(Status.COMPLETED);
		when(etlClient.requestJobStatus(job.getId().toString())).thenReturn(job);
		
		String[] args = buildCommand(new String[] {"--status", "--jobId", job.getId().toString()});
		
		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			verify(etlClient).requestJobStatus(job.getId().toString());
			assertEquals(0, e.status);
		}
	}
	
	@Test
	public void testStatusMissingJobId() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(new String[] {"--cancel"});
		try {
			Main.parseCmdlineArgs(args, etlClient);
		} catch (ExitException e) {
			assertEquals(1, e.status);
			assertEquals("Error: You must specify --jobId=<job Id>.", err.toString().trim());
		}
	}
}
