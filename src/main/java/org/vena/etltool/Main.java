package org.vena.etltool;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

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
import org.vena.api.etl.ETLMetadata;
import org.vena.api.etl.ETLMetadata.ETLLoadType;
import org.vena.etltool.entities.ETLJobDTO;
import org.vena.etltool.entities.ModelResponseDTO;
import org.vena.id.Id;

public class Main {
	private static final String EXAMPLE_COMMANDLINE = "etl-tool --host=localhost --port=8080 --apiUser=1.1 --apiKey=4d87c176227045de9628fb5f010a7b40 --file=model.csv;hierarchy";

	/**
	 * @param args
	 * @throws UnsupportedEncodingException 
	 */
	public static void main(String[] args) throws UnsupportedEncodingException {
		System.getProperties().setProperty("datacenterId", "1");

		ETLClient etlClient = new ETLClient();

		ETLMetadata metadata = parseCmdlineArgs(args, etlClient);

		System.out.print("Submitting job... ");
		ETLJobDTO etlJob = etlClient.uploadETL(metadata);
		System.out.println("OK");
		System.out.println("Job submitted. Your ETL Job Id is "+etlJob.getId());
		
		/* If polling option was provided, poll until the task completes. */
		if( etlClient.pollingRequested  ) {
			System.out.println("Waiting for job to finish... ");
			etlClient.pollTillJobComplete(etlJob.getId());
			System.out.println("Done.");
		}
	}


	@SuppressWarnings("static-access")
	private  static ETLMetadata parseCmdlineArgs(String[] args, ETLClient etlClient) throws UnsupportedEncodingException {
		Options options  = new Options();

		Option helpOption =  OptionBuilder
				.withLongOpt( "help")
				.withDescription("Print this message" )
				.isRequired(false)
				.create();

		options.addOption(helpOption);

		Option versionOption =  OptionBuilder
				.withLongOpt( "version")
				.withDescription("Print the version of the cmdline tool." )
				.isRequired(false)
				.create();

		options.addOption(versionOption);

		Option statusOption =  OptionBuilder
				.withLongOpt( "status")
				.withDescription("Request status for the specified etlJob.  Requires --jobId option." )
				.isRequired(false)
				.create();

		options.addOption(statusOption);

		Option sslOption = 
				OptionBuilder
				.withLongOpt("ssl")
				.isRequired(false)
				.withDescription("Use SSL encryption to connect to the server (this is default).")
				.create();

		options.addOption(sslOption);

		Option noSSLOption = 
				OptionBuilder
				.withLongOpt("nossl")
				.isRequired(false)
				.withDescription("Don't use SSL encryption to connect to the server.")
				.create();

		options.addOption(noSSLOption);

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
				.withDescription("The hostname of the API server to connect to.  Defaults to proxy.vena.io.")
				.create();

		options.addOption(hostOption);

		Option portOption = 
				OptionBuilder
				.withLongOpt("port")
				.isRequired(false)
				.hasArg()
				.withDescription("The port to connect to on the API server.  Defaults to 443 with SSL or 80 without SSL.")
				.create();

		portOption.setType(Integer.class);
		options.addOption(portOption);

		Option modelId = 
				OptionBuilder
				.withLongOpt("modelId")
				.isRequired(false)
				.hasArg()
				.withDescription("The Id of the model to apply the etl job to. See also --modelName.")
				.create();

		options.addOption(modelId);

		Option modelName = 
				OptionBuilder
				.withLongOpt("modelName")
				.isRequired(false)
				.hasArg()
				.withDescription("The name of the model to apply the etl job to. See also --modelId.")
				.create();

		options.addOption(modelName);

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
				.withDescription("An ETL file to add to the ETL job. -F<filename>;<filetype>[;<tableName>]. <filetype> is one of {"+ETLFile.SUPPORTED_FILETYPES_LIST+"}>. <tableName> is only required if <filetype> is 'user_defined'. Example: -F intersections.csv;intersections")
				.create('F');

		options.addOption(fileOption);

		Option stageOption = 
				OptionBuilder
				.withLongOpt("stage")
				.isRequired(false)
				.withDescription("Load the files into the SQL staging area and await SQL transform.")
				.create();

		options.addOption(stageOption);

		Option stage2Option = 
				OptionBuilder
				.withLongOpt("stageAndTransform")
				.isRequired(false)
				.withDescription("Load the files into the SQL staging area and await SQL transform.")
				.create();

		options.addOption(stage2Option);

		Option stageOnlyOption = 
				OptionBuilder
				.withLongOpt("stageOnly")
				.isRequired(false)
				.withDescription("Load the files into the SQL staging area.")
				.create();

		options.addOption(stageOnlyOption);

		Option transformCompleteOption = 
				OptionBuilder
				.withLongOpt("transformComplete")
				.isRequired(false)
				.withDescription("Signal to the server that SQL transform is complete and to start loading from the SQL staging area. Requires --jobId option.")
				.create();

		options.addOption(transformCompleteOption);

		Option loadFromStagingOption = 
				OptionBuilder
				.withLongOpt("loadFromStaging")
				.isRequired(false)
				.withDescription("Load data from the SQL staging area. This creates a new job ID.")
				.create();

		options.addOption(loadFromStagingOption);

		Option setErrorOption = 
				OptionBuilder
				.withLongOpt("setError")
				.isRequired(false)
				.hasOptionalArg()
				.withDescription("Set the job status to error with optional error message. Requires --jobId option.")
				.create();

		options.addOption(setErrorOption);

		Option cancelOption = 
				OptionBuilder
				.withLongOpt("cancel")
				.isRequired(false)
				.withDescription("Request a job to be cancelled. Requires --jobId option.")
				.create();

		options.addOption(cancelOption);

		Option jobIdOption = 
				OptionBuilder
				.withLongOpt("jobId")
				.isRequired(false)
				.hasArg()
				.withDescription("Specify a job ID (for certain operations). Example: --jobId=79026536904130560")
				.create();

		options.addOption(jobIdOption);

		Option jobNameOption = 
				OptionBuilder
				.withLongOpt("jobName")
				.isRequired(false)
				.hasArg()
				.withDescription("Specify a job name (when creating a new job only)")
				.create();

		options.addOption(jobNameOption);

		Option templateOption = 
				OptionBuilder
				.withLongOpt("templateId")
				.isRequired(false)
				.hasArg()
				.withDescription("Specify a template ID to associate with this template")
				.create();

		options.addOption(templateOption);

		Option validateOption = 
				OptionBuilder
				.withLongOpt("validate")
				.isRequired(false)
				.withDescription("Validate the ETL.  Performs a dry run without saving data, and sends back a list of validation results.")
				.create();

		options.addOption(validateOption);

		Option exportOption = 
				OptionBuilder
				.withLongOpt("export")
				.isRequired(false)
				.hasArg()
				.withArgName("type")
				.withDescription("Export part of the datamodel to a staging table. <type> may be one of {"+ETLFile.SUPPORTED_FILETYPES_LIST+"}.")
				.create();

		options.addOption(exportOption);

		Option exportStagingOption = 
				OptionBuilder
				.withLongOpt("exportToTable")
				.isRequired(false)
				.hasArg()
				.withArgName("name")
				.withDescription("Name of table in staging DB to export to.")
				.create();

		options.addOption(exportStagingOption);

		Option exportFileOption = 
				OptionBuilder
				.withLongOpt("exportToFile")
				.isRequired(false)
				.hasArg()
				.withArgName("name")
				.withDescription("Name of file to export to.")
				.create();

		options.addOption(exportFileOption);

		Option exportWhereOption = 
				OptionBuilder
				.withLongOpt("exportWhere")
				.isRequired(false)
				.hasArg()
				.withDescription("Where clause for export (HQL). May not be combined with --exportQuery.")
				.create();

		options.addOption(exportWhereOption);

		Option exportQueryOption = 
				OptionBuilder
				.withLongOpt("exportQuery")
				.isRequired(false)
				.hasArg()
				.withDescription("Query expression for export (model slice language).  May not be combined with --exportWhere.")
				.create();

		options.addOption(exportQueryOption);

		Option excludeHeadersOption = 
				OptionBuilder
				.withLongOpt("excludeHeaders")
				.isRequired(false)
				.withDescription("Exclude header row when exporting to file.")
				.create();

		options.addOption(excludeHeadersOption);

		Option deleteOption = 
				OptionBuilder
				.withLongOpt("delete")
				.isRequired(false)
				.hasArg()
				.withArgName("type")
				.withDescription("Delete all <type> from the datamodel that matches --deleteQuery. <type> can be one of {intersections}.")
				.create();

		options.addOption(deleteOption);

		Option deleteQueryOption = 
				OptionBuilder
				.withLongOpt("deleteQuery")
				.isRequired(false)
				.hasArg()
				.withArgName("expr")
				.withDescription("The query expression to match for --delete.")
				.create();

		options.addOption(deleteQueryOption);

		Option waitOption = 
				OptionBuilder
				.withLongOpt("wait")
				.isRequired(false)
				.withDescription("Wait for job to complete (or fail) before returning. Returns status code 0 if the job was successful and non-zero if it failed.")
				.create();

		options.addOption(waitOption);

		Option verboseOption = 
				OptionBuilder
				.withLongOpt("verbose")
				.isRequired(false)
				.withDescription("Show the server calls made while the command runs.")
				.create();

		options.addOption(verboseOption);

		HelpFormatter helpFormatter = new HelpFormatter();

		CommandLine commandLine = null;

		CommandLineParser parser = new GnuParser();
		try {
			// parse the command line arguments
			commandLine = parser.parse( options, args );
		}
		catch( ParseException exp ) {
			System.err.println( "Error: " + exp.getMessage() );

			System.exit(1);
		}

		if(commandLine.hasOption("help") || args.length == 0) {

			helpFormatter.printHelp(EXAMPLE_COMMANDLINE, options);

			System.exit(0);
		}

		if(commandLine.hasOption("version")) {

			System.out.print(ETLClient.requestVersionInfo());
			System.exit(0);
		}



		String port = commandLine.getOptionValue("port");

		if( port != null)
			etlClient.port=Integer.parseInt(port);



		String hostname = commandLine.getOptionValue("host");

		if( hostname != null) {
			etlClient.host=hostname;
		}

		if( commandLine.hasOption("ssl") && commandLine.hasOption("nossl") ) { 
			System.err.println( "Error: ssl and nossl options cannot be combined.");

			System.exit(1);
		}

		if( commandLine.hasOption("ssl") ) { 
			etlClient.protocol = "https";
		}

		else if( commandLine.hasOption("nossl") ) { 
			etlClient.protocol = "http";
		}

		if( commandLine.hasOption("wait") ) { 
			etlClient.pollingRequested = true;
		}

		if( commandLine.hasOption("verbose") ) { 
			etlClient.verbose = true;
		}

		String apiUser =  commandLine.getOptionValue("apiUser");

		etlClient.apiUser = apiUser;

		String apiKey =  commandLine.getOptionValue("apiKey");

		etlClient.apiKey = apiKey;

		String username =  commandLine.getOptionValue("username");

		etlClient.username = username;

		String password =  commandLine.getOptionValue("password");

		etlClient.password = password;

		String jobId =  commandLine.getOptionValue("jobId");

		String templateId = commandLine.getOptionValue("templateId");

		etlClient.templateId = templateId;

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

			System.out.print("Logging in... ");
			etlClient.login();
			System.out.println("OK");
		}

		// Options that work on a single job ID

		if(commandLine.hasOption("status") || args.length == 0) {

			if (jobId == null) {
				System.err.println( "Error: You must specify --jobId=<job Id>.");
				System.exit(1);
			}

			System.out.print("Fetching job status... ");
			ETLJobDTO etlJob = etlClient.requestJobStatus(jobId);
			System.out.println("OK");

			ETLClient.printJobStatus(etlJob);

			System.exit(0);
		}

		if (commandLine.hasOption("transformComplete")) {

			if (jobId == null) {
				System.err.println( "Error: You must specify --jobId=<job Id>.");
				System.exit(1);
			}

			System.out.print("Signalling job to continue... ");
			ETLJobDTO etlJob = etlClient.sendTransformComplete(jobId);
			System.out.println("OK");
			
			/* If polling option was provided, poll until the task completes. */
			if( etlClient.pollingRequested  ) {
				System.out.println("Waiting for job to finish... ");
				etlClient.pollTillJobComplete(etlJob.getId());
				System.out.println("Done.");
			}
			
			System.exit(0);
		}

		if (commandLine.hasOption("setError")) {

			if (jobId == null) {
				System.err.println( "Error: You must specify --jobId=<job Id>.");
				System.exit(1);
			}

			System.out.print("Setting job error status... ");
			etlClient.setJobError(jobId, commandLine.getOptionValue("setError"));
			System.out.println("OK");
			System.exit(0);
		}

		if (commandLine.hasOption("cancel")) {

			if (jobId == null) {
				System.err.println( "Error: You must specify --jobId=<job Id>.");
				System.exit(1);
			}

			System.out.print("Cancelling job... ");
			etlClient.sendCancel(jobId);
			System.out.println("OK");
			System.exit(0);
		}
		
		String modelIdStr = commandLine.getOptionValue("modelId");
		String modelNameStr = commandLine.getOptionValue("modelName");

		if (commandLine.hasOption("validate")) {
			etlClient.validationRequested = true;
		}

		/* Process model parameters. Create a new model if necessary. */
		if( modelIdStr != null || modelNameStr != null) {

			if(modelIdStr != null && modelNameStr != null)  {
				System.err.println( "Error: You must specify either --modelId=<existing model Id> or --modelName=<model name>, but not both.");

				System.exit(1);
			}

			//Lookup model by name.
			if( modelNameStr != null ) {
				System.out.print("Looking up model... ");
				ModelResponseDTO searchResults = etlClient.lookupModel(modelNameStr);

				if( searchResults == null) {
					System.err.println( "Error: Could not find the model named \""+modelNameStr+"\".");

					System.exit(1);
				}

				System.out.println("OK");
				etlClient.modelId = searchResults.getId();
			}
			else {
				etlClient.modelId = Id.valueOf(modelIdStr);
			}
		}
		else if(commandLine.getOptionValue("createModel") !=null) {
			System.out.print("Creating new model... ");
			etlClient.createModel(commandLine.getOptionValue("createModel"));

			System.out.println("OK");
		}
		else {
			System.err.println( "Error: You must specify one of --createModel=<model name> or --modelId or --modelName.");

			System.exit(1);
		}

		// Options that require a data model

		if (commandLine.hasOption("export")) {
			String exportTypeStr = commandLine.getOptionValue("export");
			String exportToTable = commandLine.getOptionValue("exportToTable");
			String exportToFile = commandLine.getOptionValue("exportToFile");

			if (exportToFile != null && exportToTable != null)  {
				System.err.println( "Error: --exportToTable and --exportToFile options cannot be combined.");
				System.exit(1);
			}
			
			if (exportToFile == null && exportToTable == null) {
				System.err.println( "Error: export option requires either --exportToTable <name> or --exportToFile <name>.");
				System.exit(1);
			}

			Type type = null;
			try {
				type = ETLFile.Type.valueOf(exportTypeStr);
			}
			catch(IllegalArgumentException e) {
				System.err.println( "Error: The ETL file type \""+exportTypeStr+"\" does not exist. The known filetypes are ["+ETLFile.SUPPORTED_FILETYPES_LIST+"]");
				System.exit(1);
			}

			String whereClause = commandLine.getOptionValue("exportWhere");
			String queryExpr = commandLine.getOptionValue("exportQuery");
			
			if (whereClause != null && queryExpr != null) {
				System.err.println( "Error: exportWhere and exportQuery options cannot be combined.");
				System.exit(1);
			}

			boolean excludeHeaders = commandLine.hasOption("excludeHeaders");

			System.out.print("Running export (this might take a while)... ");
			InputStream in = etlClient.sendExport(type, exportToFile != null, exportToTable, whereClause, queryExpr, !excludeHeaders);
			if (exportToFile != null) {
				try {
					Files.copy(in, new File(exportToFile).toPath(), StandardCopyOption.REPLACE_EXISTING);
				} catch (IOException e) {
					e.printStackTrace();
					try {
						in.close();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
					System.exit(1);
				}
			}
			System.out.print("OK.");

			System.exit(0);
		}

		if (commandLine.hasOption("delete")) {
			String deleteTypeStr = commandLine.getOptionValue("delete");
			String expr = commandLine.getOptionValue("deleteQuery");

			if (expr == null) {
				System.err.println("Error: delete option requires --deleteQuery <expr>.");
				System.exit(1);
			}

			Type type = null;
			try {
				type = ETLFile.Type.valueOf(deleteTypeStr);
			}
			catch(IllegalArgumentException e) {
				System.err.println( "Error: The ETL file type \""+deleteTypeStr+"\" does not exist. The known filetypes are ["+ETLFile.SUPPORTED_FILETYPES_LIST+"]");
				System.exit(1);
			}

			System.out.print("Running delete (this might take a while)... ");
			etlClient.sendDelete(type, expr);
			System.out.print("OK.");

			System.exit(0);
		}

		// Do an Import
		return produceMetadata(commandLine, etlClient.modelId);
	}

	private static ETLMetadata produceMetadata(CommandLine commandLine, Id modelId) {

		System.out.println("Creating a new job.");

		ETLLoadType loadType = ETLLoadType.FILE_TO_CUBE;

		int numStageOptions = 0;
		
		if (commandLine.hasOption("stage") || commandLine.hasOption("stageAndTransform")) {
			loadType = ETLLoadType.FILE_TO_STAGE_TO_CUBE;
			numStageOptions++;
		}
		if (commandLine.hasOption("stageOnly")) {
			loadType = ETLLoadType.FILE_TO_STAGE;
			numStageOptions++;
		}
		if (commandLine.hasOption("loadFromStaging")) {
			loadType = ETLLoadType.STAGE_TO_CUBE;
			numStageOptions++;
		}

		if (numStageOptions > 1) {
			System.err.println( "Error: --stage, --stageAndTransform, --stageOnly, and --loadFromStaging options cannot be combined. At most one of these options can be used at a time.");
			System.exit(1);
		}

		String[] etlFileOptionValues = commandLine.getOptionValues("file");

		if (etlFileOptionValues == null && loadType != ETLLoadType.STAGE_TO_CUBE) {
			System.err.println( "Error: You must specify at least one --file option when submitting a job.");

			System.exit(1);
		}
		
		if (etlFileOptionValues != null && loadType == ETLLoadType.STAGE_TO_CUBE) {
			System.err.println( "Error: --file and --loadFromStaging options cannot be combined.");

			System.exit(1);
		}

		List<ETLFile> etlFiles = new ArrayList<>();

		if (etlFileOptionValues != null) {
			for(String etlFileOption : etlFileOptionValues)  {
				ETLFile etlFile = new ETLFile();

				String[] optionFields = etlFileOption.split(";");

				if( optionFields.length < 2) {
					System.err.println( "Error: The value \""+etlFileOption+"\" for option --file is invalid.  Please specify the filename followed by the file type. Example: -F intersections.csv;intersections");

					System.exit(1);
				}

				etlFile.setFilename(optionFields[0]);

				Type fileType;

				String fileTypeStr = optionFields[1];

				String tableName = (optionFields.length > 2) ? optionFields[2] : null;

				try {
					fileType = ETLFile.Type.valueOf(fileTypeStr);
					etlFile.setFileType(fileType);
					etlFile.setTableName(tableName);

					if (fileType == ETLFile.Type.user_defined &&  tableName == null) {
						System.err.println( "Error: The option  \""+etlFileOption+"\" you entered is invalid.  A table name is required for this type.  Please specify the filename followed by the file type and table name. Example: -F arbitrary.csv;user_defined;mytable");

						System.exit(1);
					}

					etlFiles.add(etlFile);
				}
				catch(IllegalArgumentException e) {
					System.err.println( "Error: The option \""+etlFileOption+"\" you entered is invalid.  The ETL file type \""+fileTypeStr+"\" does not exist. The supported filetypes are ["+ETLFile.SUPPORTED_FILETYPES_LIST+"]");

					System.exit(1);
				}

			}
		}

		ETLMetadata metadata = new ETLMetadata();
		metadata.setLoadType(loadType);
		metadata.addFiles(etlFiles);
		metadata.setModelId(modelId);

		String jobName =  commandLine.getOptionValue("jobName");
		metadata.setName(jobName);

		return metadata;
	}
}
