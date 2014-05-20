package org.vena.etltool;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import javax.ws.rs.core.MediaType;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.vena.api.etl.ETLFile;
import org.vena.api.etl.ETLFile.Type;
import org.vena.api.etl.ETLJob;
import org.vena.api.etl.ETLMetadata;
import org.vena.etltool.entities.CreateModelResponseDTO;
import org.vena.id.Id;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.multipart.FormDataBodyPart;
import com.sun.jersey.multipart.FormDataMultiPart;
import com.sun.jersey.multipart.impl.MultiPartWriter;

public class Main {
	private static final String EXAMPLE_COMMANDLINE = "etl-tool --host=localhost --port=8080 --apiUser=1.1 --apiKey=4d87c176227045de9628fb5f010a7b40 --file=model.csv;hierarchy";

	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ETLClient etlClient = new ETLClient();

		List<ETLFile> etlFiles = parseCmdlineArgs(args, etlClient);

		ETLMetadata metadata = new ETLMetadata();
		
		metadata.addFiles(etlFiles);

		metadata.setModelId(etlClient.modelId);

		try {

			ClientConfig jerseyClientConfig = new DefaultClientConfig();
			jerseyClientConfig.getClasses().add(MultiPartWriter.class);
			
			Client client = Client.create(jerseyClientConfig);

			client.addFilter(new HTTPBasicAuthFilter(etlClient.apiUser, etlClient.apiKey));

			WebResource webResource = client.resource(etlClient.protocol+"://"+etlClient.host+":"+etlClient.port+"/api/etl/upload");

			webResource.accept("application/json");

			FormDataMultiPart form = new FormDataMultiPart();

			ObjectMapper objectMapper = new ObjectMapper();

			byte[] metadataBytes = objectMapper.writeValueAsBytes(metadata);

			FormDataBodyPart metadataPart = new FormDataBodyPart("metadata",new ByteArrayInputStream(metadataBytes), MediaType.APPLICATION_JSON_TYPE);
			form.bodyPart(metadataPart);

			
			for(Entry<String, ETLFile> entry :  metadata.getFiles().entrySet()) {
				String key = entry.getKey();
				ETLFile etlFile = entry.getValue();
				
				etlFile.setMimePart(key);
				
				FormDataBodyPart filePart = new FormDataBodyPart(key, new FileInputStream(etlFile.getFilename()), MediaType.APPLICATION_OCTET_STREAM_TYPE);
				form.bodyPart(filePart);
			}
			
			ClientResponse response = webResource.type(MediaType.MULTIPART_FORM_DATA_TYPE).post(ClientResponse.class, form);

			switch( response.getStatus()) {
			
			case 200:
				ETLJob output = response.getEntity(ETLJob.class);

				System.out.println("Job submitted. Your ETL Job Id is "+output.getId());
				
				break;
			case 401:
				System.err.println("Access denied.  Check your credentials and try again.");
				System.exit(2);
				break;
			default:
				System.err.println("Error"+response);
				System.exit(3);
			}

		} catch (Exception e) {

			e.printStackTrace();

		}
	}

	@SuppressWarnings("static-access")
	private  static List<ETLFile> parseCmdlineArgs(String[] args, ETLClient etlClient) {
		Options options  = new Options();
		
		Option helpOption =  OptionBuilder
			.withLongOpt( "help")
			.withDescription("Print this message" )
			.isRequired(false)
			.create();
		
		options.addOption(helpOption);
		
		Option statusOption =  OptionBuilder
				.withLongOpt( "status")
				.withDescription("Request status for the specified etlJob.  Example: --status=79026536904130560." )
				.isRequired(false)
				.hasArg()
				.create();
			
		options.addOption(statusOption);
		
		Option sslOption = 
				OptionBuilder
				.withLongOpt("ssl")
				.isRequired(false)
				.withDescription("Use ssl to connect to the server.")
				.create();
		
		options.addOption(sslOption);
		
		Option apiUserOption = 
				OptionBuilder
				.withLongOpt("apiUser")
				.isRequired(false)
				.hasArg()
				.withDescription("The api user to use to access the API. Example 38450575909584901.2, (user 38450575909584901, customer 2). "+
						         "Note: This is different from the username used to login!")
				.create();
		
		options.addOption(apiUserOption);
		
		Option apiKeyOption = 
				OptionBuilder
				.withLongOpt("apiKey")
				.isRequired(false)
				.hasArg()
				.withDescription("The api key to use to access the API. Example 4d87c176227045de9628fb5f010a7b40. "+
						         "Note: This is different from the password used to login!")
				.create();
		
		options.addOption(apiKeyOption);
		
		Option usernameOption = 
				OptionBuilder
				.withLongOpt("username")
				.isRequired(false)
				.hasArg()
				.withDescription("The username to use to access the API. This is the same username you would use to login to the vena application.")
				.create('u');
		
		options.addOption(usernameOption);
		
		Option passwordOption = 
				OptionBuilder
				.withLongOpt("password")
				.isRequired(false)
				.hasArg()
				.withDescription("The password to use to access the API. This is the same password you would use to login to the vena application.")
				.create('p');
		
		options.addOption(passwordOption);
		
		Option hostOption = 
				OptionBuilder
				.withLongOpt("host")
				.isRequired(false)
				.hasArg()
				.withDescription("The hostname of the API server to connect to.  Defaults to localhost.")
				.create();
		
		options.addOption(hostOption);
		
		Option portOption = 
				OptionBuilder
				.withLongOpt("port")
				.isRequired(false)
				.hasArg()
				.withDescription("The port to connect to on the  the API server.  Defaults to 8080.")
				.create();
		
		portOption.setType(Integer.class);
		options.addOption(portOption);
		
		Option modelId = 
				OptionBuilder
				.withLongOpt("modelId")
				.isRequired(false)
				.hasArg()
				.withDescription("The Id of the model to apply the etl job to. See also --createModel.")
				.create();
		
		options.addOption(modelId);
		
		Option createModel = 
				OptionBuilder
				.withLongOpt("createModel")
				.isRequired(false)
				.hasArg()
				.withDescription("Will cause a brand new model to be created with the specified name.  See also: --modelId.")
				.create();
		
		options.addOption(createModel);
		
		Option fileOption = 
				OptionBuilder
				.withLongOpt("file")
				.isRequired(false)
				.hasArg()
				.withDescription("An ETL file to add to the ETL job. -F<filename>;<filetype ["+ETLFile.SUPPORTED_FILETYPES_LIST+"]>. Example: -F intersections.csv;intersections")
				.create('F');
		
		options.addOption(fileOption);
		
		
		HelpFormatter helpFormatter = new HelpFormatter();
		
		CommandLine commandLine = null;
		
		CommandLineParser parser = new GnuParser();
	    try {
	        // parse the command line arguments
	        commandLine = parser.parse( options, args );
	        
			if(commandLine.hasOption("help") || args.length == 0) {
				
				helpFormatter.printHelp(EXAMPLE_COMMANDLINE, options);
				
				System.exit(0);
			}
			
			
	        
	        String port = commandLine.getOptionValue("port");

	        if( port != null)
	        	etlClient.port=Integer.parseInt(port);
	        
	        String modelIdStr = commandLine.getOptionValue("modelId");
	        
	        String hostname = commandLine.getOptionValue("host");
	        
	        if( hostname != null) {
	        	etlClient.host=hostname;
	        }
	        
	        if( commandLine.hasOption("ssl") ) { 
	        	etlClient.protocol = "https";
	        }
	        
	        String apiUser =  commandLine.getOptionValue("apiUser");
	        
	        etlClient.apiUser = apiUser;
	        
	        String apiKey =  commandLine.getOptionValue("apiKey");
	        
	        etlClient.apiKey = apiKey;
	        
	        String username =  commandLine.getOptionValue("username");
	        
	        etlClient.username = username;
	        
	        String password =  commandLine.getOptionValue("password");
	        
	        etlClient.password = password;
	        
	        /* Cross validation for the authentication options. */
	        if( apiKey == null && username == null) {
	        	System.err.println( "Error: You must specify either --username/--password options  or --apiUser/--apiKey to authenticate with the server.");
		        
		        System.exit(1);
	        }
	        
	        if( (apiKey != null || apiUser != null ) ) {
	        	if(username !=null || password !=null) {
	        		System.err.println( "Error: apiKey and username/password options cannot be combined.  Use either --username and --pasword together, or --apiUser and --apiKey together.");
			        
			        System.exit(1);
	        	}
	        }
	        
	        if( (username != null || password != null ) ) {
	        	
	        	if(apiKey !=null || apiUser !=null) {
	        		System.err.println( "Error: apiKey and username/password options cannot be combined.  Use either --username and --pasword together, or --apiUser and --apiKey together.");
			        
			        System.exit(1);
	        	}
	        	
	        	etlClient.login();
	        }
	        
	        if(commandLine.hasOption("status") || args.length == 0) {
				
				String jobIdStr=commandLine.getOptionValue("status");
				
				ETLJob etlJob = etlClient.requestJobStatus(jobIdStr);
				
				System.exit(0);
			}
	        
	        /* Process model parameters. Create a new model if necessary. */
	        if( modelIdStr != null) { 
	        	etlClient.modelId = Id.valueOf(modelIdStr);
	        }
	        else if(commandLine.getOptionValue("createModel") !=null) {
	        	CreateModelResponseDTO modelResponse = etlClient.createModel(commandLine.getOptionValue("createModel"));
	        	
	        	System.out.println("Created model. Id="+modelResponse.getId());
	        }
	        else {
	        	System.err.println( "Error: You must specify either --modelId=<existing model Id> or --createModel=<model name>.");
		        
		        System.exit(1);
	        }
	        
	        
	        
			String[] etlFileOptionValues = commandLine.getOptionValues("file");
	        
			List<ETLFile> etlFiles = new ArrayList<>();
			
	        for(String etlFileOption : etlFileOptionValues)  {
	        	ETLFile etlFile = new ETLFile();
	        	
	        	String[] optionFields = etlFileOption.split(";");
	        	
	        	if( optionFields.length != 2) {
	        		System.err.println( "Error: The option  \""+etlFileOption+"\" you entered is invalid.  Please specify the filename followed by the file type. Example: -F intersections.csv;intersections");
			        
			        System.exit(1);
	        	}
	        	
	        	etlFile.setFilename(optionFields[0]);
	        	
	        	Type fileType;
	        	
	        	String fileTypeStr = optionFields[1];
	        	
				try {
	        		fileType = ETLFile.Type.valueOf(fileTypeStr);
	        		etlFile.setFileType(fileType);
	        		
	        		etlFiles.add(etlFile);
	        	}
	        	catch(IllegalArgumentException e) {
	        		System.err.println( "Error: The option \""+etlFileOption+"\" you entered is invalid.  The ETL file type \""+fileTypeStr+"\" does not exist. The supported filetypes are ["+ETLFile.SUPPORTED_FILETYPES_LIST+"]");
			        
			        System.exit(1);
	        	}
	
	        }
	        
	        return etlFiles;
	    }
	    catch( ParseException exp ) {
	        System.err.println( "Error: " + exp.getMessage() );
	        
	        helpFormatter.printHelp( EXAMPLE_COMMANDLINE, options );
	        
	        System.exit(1);
	    }
	

		
		//Needed to silence a compiler error.  Unreachable, actually.
		return null;
	}
}
