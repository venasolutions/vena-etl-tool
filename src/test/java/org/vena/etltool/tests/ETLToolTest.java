package org.vena.etltool.tests;

import static org.mockito.Mockito.*;

import java.security.Permission;
import java.util.Arrays;

import org.vena.etltool.ETLClient;
import org.vena.etltool.entities.Id;
import org.vena.etltool.entities.ModelResponseDTO;


public abstract class ETLToolTest {
	
	Id modelId;
	
	protected String[] buildCommandArgs() {
		String[] commandArgs = {"--username=admin@vena.io", "--password=vena", "--modelName=myModel", "--nossl"};
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
		modelId = new Id(15006893467402L);
		searchResults.setName("myModel");
		searchResults.setId(modelId);
		
		when(mockedETLClient.lookupModel("myModel")).thenReturn(searchResults);
		
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
    
}
