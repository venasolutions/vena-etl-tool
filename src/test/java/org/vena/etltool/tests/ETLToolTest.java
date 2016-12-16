package org.vena.etltool.tests;

import static org.mockito.Mockito.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.security.Permission;
import java.util.Arrays;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.mockito.invocation.*;
import org.mockito.stubbing.Answer;
import org.vena.etltool.ETLClient;
import org.vena.etltool.entities.ETLJobDTO;
import org.vena.etltool.entities.ETLMetadataDTO;
import org.vena.etltool.entities.Id;
import org.vena.etltool.entities.ModelResponseDTO;


public abstract class ETLToolTest {
	
	Id modelId;
	
	protected PrintStream originalSystemErr = System.err;
	protected PrintStream originalSystemOut = System.out;
	
	protected final ByteArrayOutputStream out = new ByteArrayOutputStream();
	protected final ByteArrayOutputStream err = new ByteArrayOutputStream();

	@Before
	public void setUpStreams() {
	    System.setOut(new PrintStream(out));
	    System.setErr(new PrintStream(err));
	}

	@After
	public void cleanUpStreams() {
	    System.setOut(originalSystemErr);
	    System.setErr(originalSystemOut);
	}
	
	protected String[] buildCommandArgs() {
		String[] commandArgs = {"--username=admin@vena.io", "--password=vena", "--modelName=myModel"};
		return commandArgs;
	}
	
	protected String[] buildCommand(String[] cmdArgs) {
		String[] authArgs = buildCommandArgs();
		String[] args = Arrays.copyOf(authArgs, authArgs.length + cmdArgs.length);
		
		for (int i=0; i<cmdArgs.length; i++) {
			args[authArgs.length + i] = cmdArgs[i];
		}
		
		return args;
	}
	
	protected ETLClient buildETLClient() {
		ETLClient mockedETLClient = mock(ETLClient.class);
		
		ModelResponseDTO searchResults = new ModelResponseDTO();
		modelId = uniqueId();
		searchResults.setName("myModel");
		searchResults.setId(modelId);
		
		when(mockedETLClient.lookupModel("myModel")).thenReturn(searchResults);
		when(mockedETLClient.uploadETL(any(ETLMetadataDTO.class))).thenAnswer(new Answer<ETLJobDTO>() {
		     public ETLJobDTO answer(InvocationOnMock invocation) {
		         Object[] args = invocation.getArguments();
		         ETLJobDTO job = new ETLJobDTO();
		         job.setMetadata((ETLMetadataDTO) args[0]);
		         job.setId(uniqueId());
		         return job;
		     }
		});
		
		return mockedETLClient;
	}
	
    @SuppressWarnings("serial")
	protected static class ExitException extends SecurityException {
        public final int status;
        public ExitException(int status) {
            this.status = status;
        }
    }

    private static class NoExitSecurityManager extends SecurityManager {
        @Override
        public void checkPermission(Permission perm) {}
        
        @Override
        public void checkPermission(Permission perm, Object context) {}
        
        @Override
        public void checkExit(int status) {
            super.checkExit(status);
            throw new ExitException(status);
        }
    }
    
    protected void setNoExitSecurityManager() {
    	System.setSecurityManager(new NoExitSecurityManager());
    }
    
    protected Id uniqueId() {
    	long seed = System.currentTimeMillis();
    	Random random = new Random(seed);
    	return new Id(seed + random.nextInt(10000));
    }
    
}
