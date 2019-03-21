package org.vena.etltool;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.vena.etltool.entities.*;
import org.vena.etltool.entities.ETLFileImportStepDTO.FileFormat;
import org.vena.etltool.entities.ETLCubeToStageStepDTO.QueryType;
import org.vena.etltool.entities.ETLMetadataDTO.ETLLoadType;
import org.vena.etltool.entities.ETLStepDTO.DataType;
import org.vena.etltool.entities.ETLStreamStepDTO.MockMode;
import org.apache.commons.csv.CSVParser;

public class Main {
	
	public static int FIRST_FILE_INDEX = 1;
	
	private static final String EXAMPLE_COMMANDLINE = "etl-tool "
			+ "[--host <addr>] [--port <num>] [--ssl|--nossl]"
			+ "\n{ --apiUser=<uid.cid> --apiKey=<key> "
			+ "\n| --user=<email> --password=<password>"
			+ "\n}"
			+ "\n{ --modelName <name> | --modelId <id>"
			+ "\n}"
			+ "\n{ [--queue|--noqueue]"
			+ "\n}"
			+ "\n{ --loadFromStaging [--wait|--waitFully]"
			+ "\n| [--runTemplate=<templateId>]"
			+ "\n| [--stage|--stageOnly|--venaTable] [--wait|--waitFully] [--validate] [--templateId <id>] [--jobName <name>] --file \"[file=]<filename>; [type=]<filetype> [;[table=]<tableName>] [;format={CSV|PSV|TDF}] [;bulkInsert={true|false}]\""
			+ "\n| --cancel --jobId <id>"
			+ "\n| --setError --jobId <id>"
			+ "\n| --status --jobId <id>"
			+ "\n| --transformComplete --jobId <id>"
			+ "\n| --delete <type> --deleteQuery <expr> [--nowait]"
			+ "\n| --export <type>\n {--exportQuery <expr> | --exportWhere <clause>}\n {--exportToFile <name> [--excludeHeaders] [--exportFormat {CSV|PSV|TDF}] | --exportToTable <name> [--nowait]}"
			+ "\n| --loadSteps <file>"
			+ "\n}";
	
	/**
	 * @param args
	 * @throws UnsupportedEncodingException 
	 */
	public static void main(String[] args) throws UnsupportedEncodingException {
		System.getProperties().setProperty("datacenterId", "1");

		ETLClient etlClient = new ETLClient(new JerseyClientFactory());
		CommandLine commandLine = parseCommandLineArgs(args);
		ETLMetadataDTO metadata = produceETLMetadata(etlClient, commandLine);

		System.out.print("Submitting job... ");
		ETLJobDTO etlJob = etlClient.uploadETL(metadata);
		System.out.println("OK");
		System.out.println("Job submitted. Your ETL Job Id is "+etlJob.getId());
		
		/* If polling option was provided, poll until the task completes. */
		if( etlClient.pollingRequested  ) {
			System.out.println("Waiting for job to finish... ");
			etlClient.pollTillJobComplete(etlJob.getId(), etlClient.waitFully);
			System.out.println("Done.");
		}
	}

	@SuppressWarnings("static-access")
	public static CommandLine parseCommandLineArgs(String[] args) {
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
				.withArgName("uid.cid")
				.withDescription("The api user to use to access the API. Example 38450575909584901.2, (user 38450575909584901, customer 2). "+
						"Note: This is different from the username used to login!")
						.create();

		options.addOption(apiUserOption);

		Option apiKeyOption = 
				OptionBuilder
				.withLongOpt("apiKey")
				.isRequired(false)
				.hasArg()
				.withArgName("key")
				.withDescription("The api key to use to access the API. Example 4d87c176227045de9628fb5f010a7b40. "+
						"Note: This is different from the password used to login!")
						.create();

		options.addOption(apiKeyOption);

		Option usernameOption = 
				OptionBuilder
				.withLongOpt("username")
				.isRequired(false)
				.hasArg()
				.withArgName("email")
				.withDescription("The username to use to access the API. This is the same username you would use to login to the vena application.")
				.create('u');

		options.addOption(usernameOption);

		Option passwordOption = 
				OptionBuilder
				.withLongOpt("password")
				.isRequired(false)
				.hasArg()
				.withArgName("password")
				.withDescription("The password to use to access the API. This is the same password you would use to login to the vena application.")
				.create('p');

		options.addOption(passwordOption);

		Option hostOption = 
				OptionBuilder
				.withLongOpt("host")
				.isRequired(false)
				.hasArg()
				.withArgName("addr")
				.withDescription("The hostname of the API server to connect to.  Defaults to vena.io.")
				.create();

		options.addOption(hostOption);

		Option portOption = 
				OptionBuilder
				.withLongOpt("port")
				.isRequired(false)
				.hasArg()
				.withArgName("num")
				.withDescription("The port to connect to on the API server.  Defaults to 443 with SSL or 80 without SSL.")
				.create();

		portOption.setType(Integer.class);
		options.addOption(portOption);

		Option modelId = 
				OptionBuilder
				.withLongOpt("modelId")
				.isRequired(false)
				.hasArg()
				.withArgName("id")
				.withDescription("The Id of the model to apply the etl job to. See also --modelName.")
				.create();

		options.addOption(modelId);

		Option modelName = 
				OptionBuilder
				.withLongOpt("modelName")
				.isRequired(false)
				.hasArg()
				.withArgName("name")
				.withDescription("The name of the model to apply the etl job to. See also --modelId.")
				.create();

		options.addOption(modelName);

		Option createModel = 
				OptionBuilder
				.withLongOpt("createModel")
				.isRequired(false)
				.hasArg()
				.withArgName("name")
				.withDescription("Will cause a brand new model to be created with the specified name.  See also: --modelId.")
				.create();

		options.addOption(createModel);

		Option fileOption = 
				OptionBuilder
				.withLongOpt("file")
				.isRequired(false)
				.hasArg()
				.withArgName("options")
				.withDescription("A data file to import (multiple allowed)."
						+ "\n -F \"[file=]<filename>; [type=]<filetype> [;[table=]<tableName>] [;format={CSV|PSV|TDF}] [;bulkInsert={true|false}] [;clearSlices=<expr>] "
						+ "[;clearSlicesByDimNums=<expr>] [;encoding=<fileEncoding>] [;clearSlicesByColumns=<listOfColumnNames>] \""
						+ "\n where <filetype> is one of {"+ETLFileOldDTO.SUPPORTED_FILETYPES_LIST+"}>."
						+ "\n and <expr> is the expression specifying the slice of the cube to clear intersections from. Multiple expressions separated by a comma are supported."
						+ "\n and <fileEncoding> is the type of encoding used by the file to be imported, e.g. UTF-16."
						+ "\n and <listOfColumnNames> is a comma separated list of column names to clear on."
						+ "\n Example: -F model.csv;hierarchy"
						+ "\n Example: -F file=values.tdf;format=TDF;type=intersections")
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

		Option venaTableOption =
				OptionBuilder
						.withLongOpt("venaTable")
						.isRequired(false)
						.withDescription("Load the files into a Vena Table.")
						.create();

		options.addOption(venaTableOption);

		Option setErrorOption = 
				OptionBuilder
				.withLongOpt("setError")
				.isRequired(false)
				.hasOptionalArg()
				.withArgName("msg")
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
				.withArgName("id")
				.withDescription("Specify a job ID (for certain operations). Example: --jobId=79026536904130560")
				.create();

		options.addOption(jobIdOption);

		Option jobNameOption = 
				OptionBuilder
				.withLongOpt("jobName")
				.isRequired(false)
				.hasArg()
				.withArgName("name")
				.withDescription("Specify a job name (when creating a new job only)")
				.create();

		options.addOption(jobNameOption);

		Option templateOption = 
				OptionBuilder
				.withLongOpt("templateId")
				.isRequired(false)
				.hasArg()
				.withArgName("id")
				.withDescription("Specify a template ID to associate when creating a new job")
				.create();

		options.addOption(templateOption);

		Option validateOption = 
				OptionBuilder
				.withLongOpt("validate")
				.isRequired(false)
				.withDescription("Validate the import files.  Performs a dry run without saving data, and sends back a list of validation results.")
				.create();

		options.addOption(validateOption);

		Option exportOption = 
				OptionBuilder
				.withLongOpt("export")
				.isRequired(false)
				.hasArg()
				.withArgName("type")
				.withDescription("Export part of the datamodel to a staging table. <type> may be one of {"+ETLFileOldDTO.SUPPORTED_FILETYPES_LIST+"}.")
				.create();

		options.addOption(exportOption);

		Option exportStagingOption = 
				OptionBuilder
				.withLongOpt("exportToTable")
				.isRequired(false)
				.hasArg()
				.withArgName("name")
				.withDescription("Name of table in staging DB to export to. By default, waits for the job to complete unless --nowait is specified.")
				.create();

		options.addOption(exportStagingOption);
		
		Option exportFromOption = 
				OptionBuilder
				.withLongOpt("exportFromTable")
				.isRequired(false)
				.hasArg()
				.withArgName("name")
				.withDescription("Name of table in staging DB to export from. By default, waits for the job to complete unless --nowait is specified.")
				.create();

		options.addOption(exportFromOption);


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
				.withArgName("clause")
				.withDescription("Where clause for export (HQL). May not be combined with --exportQuery.")
				.create();

		options.addOption(exportWhereOption);

		Option exportQueryOption = 
				OptionBuilder
				.withLongOpt("exportQuery")
				.isRequired(false)
				.hasArg()
				.withArgName("expr")
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

		Option exportFormatOption = 
				OptionBuilder
				.withLongOpt("exportFormat")
				.isRequired(false)
				.hasArg()
				.withDescription("File format for export to file (default CSV). Other options: PSV, TDF.")
				.create();

		options.addOption(exportFormatOption);

		Option deleteOption = 
				OptionBuilder
				.withLongOpt("delete")
				.isRequired(false)
				.hasArg()
				.withArgName("type")
				.withDescription("Delete all <type> from the datamodel that matches --deleteQuery. <type> can be one of {intersections, values, lids}.")
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

		Option clearSlicesOption =
				OptionBuilder
				.withLongOpt("clearSlices")
				.isRequired(false)
				.hasArg()
				.withArgName("expr")
				.withDescription("The expression specifying the slice of the cube to clear intersections from. Multiple expressions separated by a comma are supported.")
				.create();

		options.addOption(clearSlicesOption);
		
		Option clearSlicesByDimNumsOption =
				OptionBuilder
				.withLongOpt("clearSlicesByDimNums")
				.isRequired(false)
				.hasArg()
				.withArgName("dimensions")
				.withDescription("A list of dimension numbers separated by commas to be used to compute slices to clear.")
				.create();
		
		options.addOption(clearSlicesByDimNumsOption);

		Option clearSlicesByColumnsOption =
				OptionBuilder
				.withLongOpt("clearSlicesByColumns")
				.isRequired(false)
				.hasArg()
				.withArgName("columns")
				.withDescription("A list of columns names separated by commas to be used to compute slices to clear.")
				.create();

		options.addOption(clearSlicesByColumnsOption);
		
		Option waitOption = 
				OptionBuilder
				.withLongOpt("wait")
				.isRequired(false)
				.withDescription("Wait for job to complete (or fail) before returning. Returns status code 0 if the job was successful and non-zero if it failed. "
						+ "For jobs run with --stage or --stageAndTransform, this will only wait until the job has completed the first step (reached IN_STAGING).")
				.create();

		options.addOption(waitOption);

		Option waitFullyOption = 
				OptionBuilder
				.withLongOpt("waitFully")
				.isRequired(false)
				.withDescription("Wait for job to fully complete (or fail) before returning. Returns status code 0 if the job was successful and non-zero if it failed. "
						+ "For jobs run with --stage or --stageAndTransform, this will wait until the job has fully completed.")
				.create();

		options.addOption(waitFullyOption);

		Option noWaitOption = 
				OptionBuilder
				.withLongOpt("nowait")
				.isRequired(false)
				.withDescription("Do not wait for job to fully complete before returning. The command will return as soon as the job is submitted.")
				.create();

		options.addOption(noWaitOption);

		Option verboseOption = 
				OptionBuilder
				.withLongOpt("verbose")
				.isRequired(false)
				.withDescription("Show the server calls made while the command runs.")
				.create();

		options.addOption(verboseOption);
		
		Option loadStepsOption = 
				OptionBuilder
				.withLongOpt("loadSteps")
				.isRequired(false)
				.hasArg()
				.withArgName("fileName")
				.withDescription("Name of file containing load steps.")
				.create();

		options.addOption(loadStepsOption);
		
		Option runChannelOption =
				OptionBuilder
				.withLongOpt("runChannel")
				.isRequired(false)
				.hasArg()
				.withArgName("channelId")
				.withDescription("Id of integrations channel to run")
				.create();
		
		options.addOption(runChannelOption);
		
		Option runTemplateOption = 
				OptionBuilder
				.withLongOpt("runTemplate")
				.isRequired(false)
				.hasArg()
				.withArgName("templateId")
				.withDescription("Run the ETL template with the given id.")
				.create();

		options.addOption(runTemplateOption);

		Option queueOption =
				OptionBuilder
				.withLongOpt("queue")
				.isRequired(false)
				.withDescription("Queue this ETL job if another job is already running under this data model.")
				.create();

		options.addOption(queueOption);

		Option noQueueOption =
				OptionBuilder
				.withLongOpt("noqueue")
				.isRequired(false)
				.withDescription("Do not queue this ETL job if another job is already running under this data model.")
				.create();

		options.addOption(noQueueOption);

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

		if (commandLine.getArgList().size() > 0) {
			System.err.print( "Warning: Unrecognized argument(s):");
			for (String str : commandLine.getArgs()) {
				System.err.print(str + " ");
			}
			System.err.println();
		}

		if(commandLine.hasOption("help") || args.length == 0) {

			helpFormatter.printHelp(EXAMPLE_COMMANDLINE, options);

			System.exit(0);
		}

		if(commandLine.hasOption("version")) {

			System.out.print(ETLClient.requestVersionInfo());
			System.exit(0);
		}
		return commandLine;
	}

	private static boolean prepareETLClient(ETLClient etlClient, CommandLine commandLine) {
		String port = commandLine.getOptionValue("port");

		if( port != null)
			etlClient.port=Integer.parseInt(port);



		String hostname = commandLine.getOptionValue("host");

		if( hostname != null) {
			etlClient.host=hostname;
		}

		if( commandLine.hasOption("ssl") && commandLine.hasOption("nossl") ) { 
			System.err.println( "Error: --ssl and --nossl options cannot be combined.");

			System.exit(1);
		}

		if( commandLine.hasOption("ssl") ) { 
			etlClient.protocol = "https";
		}

		else if( commandLine.hasOption("nossl") ) { 
			etlClient.protocol = "http";
		}

		if( commandLine.hasOption("nowait") && ( commandLine.hasOption("wait") || commandLine.hasOption("waitFully") ) ) { 
			System.err.println( "Error: --wait/--waitFully and --nowait options cannot be combined.");

			System.exit(1);
		}

		if (commandLine.hasOption("export") || commandLine.hasOption("delete")) {
			// For these commands, default is wait
			etlClient.pollingRequested = true;
			etlClient.waitFully = true;
		}

		if( commandLine.hasOption("wait") ) { 
			etlClient.pollingRequested = true;
			etlClient.waitFully = false;
		}

		if( commandLine.hasOption("waitFully") ) { 
			etlClient.pollingRequested = true;
			etlClient.waitFully = true;
		}

		if( commandLine.hasOption("nowait") ) { 
			etlClient.pollingRequested = false;
			etlClient.waitFully = false;
		}

		if( commandLine.hasOption("verbose") ) { 
			etlClient.verbose = true;
		}

		String apiUser =  commandLine.getOptionValue("apiUser");

		String apiKey =  commandLine.getOptionValue("apiKey");

		String username =  commandLine.getOptionValue("username");

		String password =  commandLine.getOptionValue("password");
		
		boolean runTemplate = commandLine.hasOption("runTemplate");

		if(runTemplate) {
			String[] incompatibleOptions = new String[]{"templateId", "clearSlices", "clearSlicesByDimNums",
					"loadSteps", "runChannel", "export", "exportToTable", "exportFromTable",
					"exportToFile", "exportWhere", "exportQuery", "delete", "deleteQuery",
					"jobName", "jobId", "cancel", "loadFromStaging", "stage", "stageAndTransform",
					"stageOnly", "transformComplete", "loadFromStaging", "venaTable"};
			for(String option : incompatibleOptions) {
				if(commandLine.hasOption(option)) {
					System.err.println( "Error: You cannot use --runTemplate with --" +option+".");
					System.exit(1);
				}
			}
			etlClient.templateId = commandLine.getOptionValue("runTemplate");
		} else {
			etlClient.templateId = commandLine.getOptionValue("templateId");
		}

		/* Cross validation for the authentication options. */
		if (etlClient.verbose) {
			System.out.print("Auth options found:");
			if (username != null) System.out.print(" username");
			if (password != null) System.out.print(" password");
			if (apiUser != null) System.out.print(" apiUser");
			if (apiKey != null) System.out.print(" apiKey");
			System.out.println();
		}

		if( (username != null && password != null && apiUser == null && apiKey == null) ) {
			System.out.print("Logging in... ");
			etlClient.username = username;
			etlClient.password = password;
			etlClient.login();
			System.out.println("OK");
		}
		else if( (username == null && password == null && apiUser != null && apiKey != null) ) {
			if (hostname == null) {
				System.err.println("Error: You must specify --host when using --apiUser/--apiKey.");
				System.exit(1);
			}

			// Use API key
			etlClient.apiUser = apiUser;
			etlClient.apiKey = apiKey;
		}
		else {
			System.err.println( "Error: You must specify either --username/--password or --apiUser/--apiKey to authenticate with the server, but not both.");
			System.exit(1);
		}
		return runTemplate;
	}

	public static ETLMetadataDTO produceETLMetadata(
			ETLClient etlClient, 
			CommandLine commandLine) throws UnsupportedEncodingException {
		boolean runningTemplate = prepareETLClient(etlClient, commandLine);
		ETLMetadataDTO metadata;
		if(runningTemplate) {
			if(etlClient.templateId == null) {
				System.err.println("Template Id must be specified if running an ETL template");
				System.exit(1);
			}
			System.out.print("Getting template... ");
			ETLTemplateDTO template = etlClient.getETLTemplate();
			System.out.println("OK");
			metadata = fillTemplateMetadata(commandLine, template);
			etlClient.modelId = metadata.getModelId();
		} else {
			metadata = buildETLMetadata(commandLine, etlClient);
		}
		return metadata;
	}

	public static ETLMetadataDTO buildETLMetadata(String[] args, ETLClient etlClient) throws UnsupportedEncodingException {
		CommandLine commandLine = parseCommandLineArgs(args);
		prepareETLClient(etlClient, commandLine);
		return buildETLMetadata(commandLine, etlClient);
	}
	
	public static ETLMetadataDTO buildETLMetadata(CommandLine commandLine, ETLClient etlClient) throws UnsupportedEncodingException {

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

		// Options that work on a single job ID, model ID optional
		String jobId =  commandLine.getOptionValue("jobId");
		if(commandLine.hasOption("status")) {

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
				etlClient.pollTillJobComplete(etlJob.getId(), etlClient.waitFully);
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

		// After this point, model ID is required

		if (etlClient.modelId == null) {
			System.err.println( "Error: You must specify one of --createModel=<model name> or --modelId or --modelName.");

			System.exit(1);
		}

		if (commandLine.hasOption("queue") && commandLine.hasOption("noqueue")) {
			System.err.println( "Error: --queue and --noqueue options cannot be combined.");
			System.exit(1);
		}

		// ETL 2.0 option for providing multiple steps at a time
		
		if (commandLine.hasOption("loadSteps")) {
			if (commandLine.getOptionValue("loadSteps") == null) {
				System.err.println( "Error: loadSteps option requires --loadSteps <fileName>.");
				System.exit(1);
			}
			return produceStepsMetadata(etlClient, commandLine);
		}

		// Options that require a data model

		if (commandLine.hasOption("export") || commandLine.hasOption("exportFromTable")) {
			String exportTypeStr = commandLine.getOptionValue("export");
			String queryExpr = commandLine.getOptionValue("exportQuery");
			String exportFromTable = commandLine.getOptionValue("exportFromTable");
			String exportToTable = commandLine.getOptionValue("exportToTable");
			String exportToFile = commandLine.getOptionValue("exportToFile");
			if (commandLine.hasOption("export") && commandLine.hasOption("exportFromTable")) {
				if (!exportTypeStr.equals("staging")) {
					System.err.println("Error: --export <" + exportTypeStr + "> is not supported with --exportFromTable");
					System.exit(1);
				}
			}
			DataType type = null;

			if (commandLine.hasOption("exportFromTable")){
				type = DataType.user_defined;
			}

			if (exportToFile != null && exportToTable != null)  {
				System.err.println( "Error: --exportToTable and --exportToFile options cannot be combined.");
				System.exit(1);
			}
			
			if (exportToFile == null && exportToTable == null) {
				System.err.println( "Error: export option requires either --exportToTable <name> or --exportToFile <name>.");
				System.exit(1);
			}
			
			if (exportFromTable != null && exportToTable != null){
				System.err.println( "Error: cannot export from table to another table.");
				System.exit(1);
			}
			
			if(exportFromTable != null && queryExpr != null){
				System.err.println("Error: cannot use --exportQuery with --exportFromTable. Use --exportWhere \"<HQL Query>\" instead. ");
				System.exit(1);
			}

			try {
				if (type == null) {
					type = DataType.valueOf(exportTypeStr);
				}
			}
			catch(IllegalArgumentException e) {
				System.err.println( "Error: The ETL file type \""+exportTypeStr+"\" does not exist. The known filetypes are ["+ETLFileOldDTO.SUPPORTED_FILETYPES_LIST+"]");
				System.exit(1);
			}

			String whereClause = commandLine.getOptionValue("exportWhere");
			
			
			if (whereClause != null && queryExpr != null) {
				System.err.println( "Error: exportWhere and exportQuery options cannot be combined.");
				System.exit(1);
			}

			boolean excludeHeaders = commandLine.hasOption("excludeHeaders");

			String fileFormatStr = commandLine.getOptionValue("exportFormat");
			FileFormat fileFormat = FileFormat.CSV;

			if (fileFormatStr != null) {
				try {
					fileFormat = FileFormat.valueOf(fileFormatStr);
				}
				catch(IllegalArgumentException e) {
					System.err.println( "Error: The export file format \""+fileFormatStr+"\" does not exist. The known formats are "+Arrays.asList(FileFormat.values())+"");
					System.exit(1);
				}
			}

			if (exportToFile != null) {
				System.out.print("Running export (this might take a while)... ");
				InputStream in = etlClient.sendExport(type, exportFromTable, exportToTable, whereClause, queryExpr, !excludeHeaders, fileFormat);
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
				System.out.print("OK.");
				System.exit(0);
				
			} else {
				System.out.println("Creating a new job.");
				
				ETLMetadataDTO metadata = new ETLMetadataDTO();
					
				metadata.setSchemaVersion(2);
				metadata.setModelId(etlClient.modelId);

				if (commandLine.hasOption("queue")) {
					metadata.setQueuingEnabled(true);
				}

				if (commandLine.hasOption("noqueue")) {
					metadata.setQueuingEnabled(false);
				}

				ETLCubeToStageStepDTO step = new ETLCubeToStageStepDTO();
				
				step.setDataType(type);
				step.setTableName(exportToTable);
				if (whereClause != null) {
					step.setQueryType(QueryType.HQL);
					step.setQueryString(whereClause);
				} else if (queryExpr != null) {
					step.setQueryType(QueryType.MODEL_SLICE);
					step.setQueryString(queryExpr);
				} else if ((type == DataType.intersections) || (type == DataType.lids)) {
					step.setQueryType(QueryType.MODEL_SLICE);
				}
				metadata.addStep(step);

				String jobName =  commandLine.getOptionValue("jobName");
				metadata.setName(jobName);
				
				return metadata;
			}			

		}

		if (commandLine.hasOption("delete")) {
			String deleteTypeStr = commandLine.getOptionValue("delete");
			String expr = commandLine.getOptionValue("deleteQuery");

			if (expr == null) {
				System.err.println("Error: delete option requires --deleteQuery <expr>.");
				System.exit(1);
			}

			DataType type = null;
			ETLMetadataDTO metadata = new ETLMetadataDTO();

			if (commandLine.hasOption("queue")) {
				metadata.setQueuingEnabled(true);
			}

			if (commandLine.hasOption("noqueue")) {
				metadata.setQueuingEnabled(false);
			}

			try {
				type = DataType.valueOf(deleteTypeStr);
				if (type.equals(DataType.intersections)) {
					ETLDeleteIntersectionsStepDTO step = new ETLDeleteIntersectionsStepDTO();
					step.setDataType(type);
					step.setExpression(expr);
					metadata.addStep(step);
				}
				else if (type.equals(DataType.values)) {
					ETLDeleteValuesStepDTO step = new ETLDeleteValuesStepDTO();
					step.setDataType(type);
					step.setExpression(expr);
					metadata.addStep(step);
				}
				else if (type.equals(DataType.lids)) {
					ETLDeleteLidsStepDTO step = new ETLDeleteLidsStepDTO();
					step.setDataType(type);
					step.setExpression(expr);
					metadata.addStep(step);
				}
				else { 
					throw new IllegalArgumentException();
				}
			}
			catch(IllegalArgumentException e) {
				System.err.println( "Error: The ETL file type \""+deleteTypeStr+"\" is not supported. The supported filetypes are intersections, values, and lids.");
				System.exit(1);
			}

			System.out.println("Creating a new job.");

			metadata.setSchemaVersion(2);
			metadata.setModelId(etlClient.modelId);

			String jobName = commandLine.getOptionValue("jobName");
			metadata.setName(jobName);

			return metadata;
		}
		
		if (commandLine.hasOption("runChannel")) {
			String[] channelIds = commandLine.getOptionValues("runChannel");
			
			ETLMetadataDTO metadata = new ETLMetadataDTO();
			System.out.println("Creating a new streaming job.");

			for (String channelId: channelIds) {
				try {
					ETLStreamChannelStepDTO step = new ETLStreamChannelStepDTO(Id.valueOf(channelId), MockMode.LIVE);
					metadata.addStep(step);
				} catch (NumberFormatException e) {
					System.err.println("Error: channelId could not be parsed as a number.");
					System.exit(1);
				}
			}

			metadata.setSchemaVersion(2);
			metadata.setModelId(etlClient.modelId);

			String jobName = commandLine.getOptionValue("jobName");
			metadata.setName(jobName);

			return metadata;
		}

		// Do an Import
		return produceImportMetadata(commandLine, etlClient.modelId);
	}

	private static ETLMetadataDTO produceStepsMetadata(ETLClient etlClient, CommandLine commandLine) {
		
		System.out.println("Creating a new job.");
		
		ETLMetadataDTO metadata = new ETLMetadataDTO();

		String jobName =  commandLine.getOptionValue("jobName");
		metadata.setName(jobName);
		metadata.setSchemaVersion(2);
		metadata.setModelId(etlClient.modelId);

		if (commandLine.hasOption("queue")) {
			metadata.setQueuingEnabled(true);
		}

		if (commandLine.hasOption("noqueue")) {
			metadata.setQueuingEnabled(false);
		}
		
		String stepsFile = commandLine.getOptionValue("loadSteps");
		
		try (BufferedReader br = new BufferedReader(new FileReader(new File(stepsFile).getPath()))) {
				
				String line;
				while ((line = br.readLine()) != null) {
					System.out.println(line);
					String[] optionFields = line.trim().split(" ", 2);
					String loadType = optionFields[0];

					ETLFileOldDTO etlFile = null;

					switch (loadType.toUpperCase()) {
					case "FILETOCUBE":
					{
						etlFile = prepareFilesToLoad(optionFields);
						validateNonEmptyFileType(etlFile);
						metadata.addStep(new ETLFileToCubeStepDTO(etlFile));
						break;
					}
					case "FILETOSTAGE":
					{
						etlFile = prepareFilesToLoad(optionFields);
						validateNonEmptyFileType(etlFile);
						metadata.addStep(new ETLFileToStageStepDTO(etlFile));
						break;
					}
					case "SQLTRANSFORM":
					{
						metadata.addStep(new ETLSQLTransformStepDTO());
						break;
					}
					case "STAGETOCUBE":
					{
						if (optionFields.length == 1) {
							metadata.addStep(new ETLStageToCubeStepDTO(DataType.hierarchy));
							metadata.addStep(new ETLStageToCubeStepDTO(DataType.attributes));
							metadata.addStep(new ETLStageToCubeStepDTO(DataType.intersections));
							metadata.addStep(new ETLStageToCubeStepDTO(DataType.lids));
						} else if (optionFields.length == 2) {
							ETLStageToCubeStepDTO step = new ETLStageToCubeStepDTO();

							String[] subOptionFields = optionFields[1].split(";");
							for (String field: subOptionFields) {
								String[] parts = field.split("=", 2);
								String key = parts[0].trim();
								String value = parts[1].replaceAll("\"", "").trim();
								if (key.equalsIgnoreCase("type")) {
									try {
										step.setDataType(DataType.valueOf(DataType.class, value));
									} catch (IllegalArgumentException e) {
										System.err.println( "Error: The ETL file type \""+value+"\" does not exist. The known filetypes are ["+ETLFileOldDTO.SUPPORTED_FILETYPES_LIST+"]");
										System.exit(1);
									}
								} else if (key.equalsIgnoreCase("clearSlices")) {
									if (step.getDataType() == DataType.intersections && value != null) {
										step.setClearSlicesExpressions(Arrays.asList(value.split(",")));
									} else {
										System.err.println("Error: The clearSlices option can only be combined with the type: {intersections}. Please specify the type first.");
										System.exit(1);
									}
								} else if (key.equalsIgnoreCase("clearSlicesByDimNums")) {
									if (step.getDataType() == DataType.intersections && value != null) {
										step.setClearSlicesDimensions(parseClearSlicesByDimNumsArgs(value));
									} else {
										System.err.println(
												"Error: The clearSlicesByDimNums option can only be combined with the type: {intersections}. Please specify the type first.");
										System.exit(1);
									}
								} else {
									System.err.println(
											"Error: stageToCube valid options are type=<type>, clearSlices=<expr>, and clearSlicesByDimNums=<dimensions>"
											+ "\nwhere the known types are [" + ETLFileOldDTO.SUPPORTED_FILETYPES_LIST + "], "
											+ "expression is a model query defining one or multiple slices separated by commas, "
											+ "and dimensions is a list of dimension numbers separated by commas.");
									System.exit(1);
								}
							}
							metadata.addStep(step);
						} else {
							System.err.println( "Error: stageToCube valid option is type=<type>. The known types are ["+ETLFileOldDTO.SUPPORTED_FILETYPES_LIST+"]");
							System.exit(1);
						}
						break;
					}
					case "CUBETOSTAGE":
					{
						ETLCubeToStageStepDTO step = new ETLCubeToStageStepDTO();

						if (optionFields.length != 2)
						{
							System.err.println( "Error: cubeToStage step requires type=<type>;table=<name>. The known types are ["+ETLFileOldDTO.SUPPORTED_FILETYPES_LIST+"]");
							System.exit(1);
						}

						String[] fields = optionFields[1].split(";");

						boolean typeFound = false;
						boolean tableFound = false;

						for (String field : fields) {
							String[] parts = field.split("=", 2);

							if (parts.length == 2) {
								String key = parts[0].trim();
								String value = parts[1].trim();

								switch (key) {
								case "type":
									try {
										step.setDataType(DataType.valueOf(DataType.class, value));
										typeFound = true;
									} catch (IllegalArgumentException e) {
										throw new IllegalArgumentException("The ETL file type \""+value+"\" does not exist. The supported filetypes are ["+ETLFileOldDTO.SUPPORTED_FILETYPES_LIST+"]");
									}
									break;
								case "table":
									step.setTableName(value);
									tableFound = true;
									break;
								case "exportQuery":
									step.setQueryType(QueryType.MODEL_SLICE);
									step.setQueryString(value);
									break;
								case "exportWhere":
									step.setQueryType(QueryType.HQL);
									step.setQueryString(value);
									break;
								default:
									throw new IllegalArgumentException("Unsupported key " + key);
								}
							} else {
								throw new IllegalArgumentException("The field "+ field +" contained "+ parts.length +" parts.");
							}
						}

						if (!typeFound || !tableFound) {
							System.err.println( "Error: cubeToStage step requires type=<type>;table=<name>. The known types are ["+ETLFileOldDTO.SUPPORTED_FILETYPES_LIST+"]");
							System.exit(1);
						}

						if (step.getQueryType() == null) {
							if ((step.getDataType() == DataType.intersections) || (step.getDataType() == DataType.lids))
								step.setQueryType(QueryType.MODEL_SLICE);
						}

						metadata.addStep(step);
						break;
					}
					case "INTEGRATIONCHANNEL": {
						if (optionFields.length != 2) {
							System.err.println("Error: integrationChannel step requires channelId=<channelId>.");
							System.exit(1);
						}

						try {
							ETLStreamChannelStepDTO step = new ETLStreamChannelStepDTO(Id.valueOf(optionFields[1].trim()), MockMode.LIVE);
							metadata.addStep(step);
						} catch (NumberFormatException e) {
							System.err.println("Error: channelId could not be parsed as a number.");
							System.exit(1);
						}

						break;
					}
					case "FILETOVENATABLE": {
						etlFile = prepareFilesToLoad(optionFields);
						validateFileToVenaStep(etlFile);
						metadata.addStep(new ETLFileToVenaTableStepDTO(etlFile));
						break;
					}
					case "":
						break;
					default:
						System.err.println("Error: loadType " + loadType + " not supported. "
								+ "Supported options are { fileToCube, fileToStage, SQLTransform, stageToCube, cubeToStage, integrationChannel }.");
						System.exit(1);
					}
				}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		return metadata;
	}

	private static ETLFileOldDTO prepareFilesToLoad(String[] optionFields) {
		
		if (optionFields.length < 2)
		{
			System.err.println( "\nPlease specify the filename followed by the file type, table name, and optional arguments."
					+ "\n Example: --file intersections.csv;intersections"
					+ "\n Example: --file arbitrary.csv;user_defined;mytable;bulkInsert=true");
			System.err.println( "\nOr specify options in any order using key-value pairs."
					+ "\n Example: --file \"file=arbitrary.csv; type=user_defined; table=mytable; format=CSV; bulkInsert=true\"");
			System.exit(1);
		}
		
		ETLFileOldDTO etlFile = null;
		try {
			etlFile = parseETLFileArgs(optionFields[1].replaceAll("\"", ""));
			String key = "file" + (FIRST_FILE_INDEX++);			
			etlFile.setMimePart(key);
		}
		catch (IllegalArgumentException e) {
			System.err.println( "Error: The value \"" + optionFields[1] + "\" is invalid for a file loading step.  " + e.getMessage());
			System.err.println( "\nPlease specify the filename followed by the file type, table name, and optional arguments."
					+ "\n Example: --file intersections.csv;intersections"
					+ "\n Example: --file arbitrary.csv;user_defined;mytable;bulkInsert=true");
			System.err.println( "\nOr specify options in any order using key-value pairs."
					+ "\n Example: --file \"file=arbitrary.csv; type=user_defined; table=mytable; format=CSV; bulkInsert=true\"");
			System.exit(1);
		}
		
		return etlFile;
	}

	private static ETLMetadataDTO produceImportMetadata(CommandLine commandLine, Id modelId) {

		ETLLoadType loadType = ETLLoadType.FILE_TO_CUBE;

		int numStageOrVenaTableOptions = 0;
		
		if (commandLine.hasOption("stage") || commandLine.hasOption("stageAndTransform")) {
			loadType = ETLLoadType.FILE_TO_STAGE_TO_CUBE;
			numStageOrVenaTableOptions++;
		}
		if (commandLine.hasOption("stageOnly")) {
			loadType = ETLLoadType.FILE_TO_STAGE;
			numStageOrVenaTableOptions++;
		}
		if (commandLine.hasOption("venaTable")) {
			loadType = ETLLoadType.FILE_TO_VENA_TABLE;
			numStageOrVenaTableOptions++;
		}
		if (commandLine.hasOption("loadFromStaging")) {
			loadType = ETLLoadType.STAGE_TO_CUBE;
			numStageOrVenaTableOptions++;
		}

		if (numStageOrVenaTableOptions > 1) {
			System.err.println( "Error: --stage, --stageAndTransform, --stageOnly, --loadFromStaging, and --venaTable options cannot be combined. At most one of these options can be used at a time.");
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

		if (commandLine.hasOption("clearSlices") || commandLine.hasOption("clearSlicesByDimNums")) {
			if (loadType == ETLLoadType.FILE_TO_STAGE) {
				System.err.println("Error: --clearSlices and --clearSlicesByDimNums options cannot be combined with the --stageOnly option.");
				System.exit(1);
			} else if (loadType == ETLLoadType.FILE_TO_CUBE) {
				System.err.println("Error: --clearSlices and --clearSlicesByDimNums options cannot be combined with the --file option. Instead use the suboptions clearSlices and clearSlicesByDimNums for the --file option");
				System.exit(1);
			}
		}

		if (commandLine.hasOption("clearSlicesByColumns")) {
			System.err.println("Error: clearSlicesByColumns can only be used as a suboption to the --file option for ETL File to Vena Table imports.");
			System.exit(1);
		}

		List<ETLFileOldDTO> etlFiles = handleFileOptions(etlFileOptionValues);
		for(ETLFileOldDTO etlFile : etlFiles) {
			if (loadType != ETLLoadType.FILE_TO_VENA_TABLE){
				validateNonEmptyFileType(etlFile);
			}
			if (!etlFile.getClearSlicesColumns().isEmpty() && loadType != ETLLoadType.FILE_TO_VENA_TABLE) {
				System.err.println("Error: the --file option clearSlicesByColumns is only available for ETL File to Vena Table imports.");
				System.exit(1);
			}
			if (etlFile.getClearSlicesExpressions() != null || etlFile.getClearSlicesDimensions() != null) {
				if (loadType != ETLLoadType.FILE_TO_CUBE) {
					System.err.println("Error: the --file options clearSlices and clearSlicesByDimNums are only available for ETL File to Cube imports."
							+"\n For Stage operations, use the stand alone --clearSlices and --clearSlicesByDimNums options instead.");
				}
			}
			
		}
		
		System.out.println("Creating a new job.");

		ETLMetadataDTO metadata = new ETLMetadataDTO();

		if (commandLine.hasOption("queue")) {
			metadata.setQueuingEnabled(true);
		}

		if (commandLine.hasOption("noqueue")) {
			metadata.setQueuingEnabled(false);
		}

		List<String> clearSlicesExprs = null;
		if (commandLine.hasOption("clearSlices")) {
			String clearSlicesExpr = commandLine.getOptionValue("clearSlices");
			if (clearSlicesExpr != null) {
				clearSlicesExprs = Arrays.asList(clearSlicesExpr.split(","));
			}
		}
		
		Set<Integer> clearSlicesDimNums = null;
		if (commandLine.hasOption("clearSlicesByDimNums")) {
			clearSlicesDimNums = parseClearSlicesByDimNumsArgs(commandLine.getOptionValue("clearSlicesByDimNums"));
		}
		
		switch(loadType) {
		case FILE_TO_CUBE:
			for(ETLFileOldDTO file : etlFiles) {
				metadata.addStep(new ETLFileToCubeStepDTO(file));
			}
			break;
		case FILE_TO_STAGE:
			for(ETLFileOldDTO file : etlFiles) {
				metadata.addStep(new ETLFileToStageStepDTO(file));
			}
			break;
		case FILE_TO_VENA_TABLE:
				for (ETLFileOldDTO file : etlFiles) {
					validateFileToVenaStep(file);
					metadata.addStep(new ETLFileToVenaTableStepDTO(file));
				}
			break;
		case FILE_TO_STAGE_TO_CUBE:
			for(ETLFileOldDTO file : etlFiles) {
				metadata.addStep(new ETLFileToStageStepDTO(file));
			}
			metadata.addStep(new ETLSQLTransformStepDTO(etlFiles, null));
			// fall-through:
		case STAGE_TO_CUBE:
			metadata.addStep(new ETLStageToCubeStepDTO(DataType.hierarchy));
			metadata.addStep(new ETLStageToCubeStepDTO(DataType.attributes));
			metadata.addStep(new ETLStageToCubeStepDTO(DataType.intersections, clearSlicesExprs, clearSlicesDimNums));
			metadata.addStep(new ETLStageToCubeStepDTO(DataType.lids));
			break;
		}
		
		metadata.setSchemaVersion(2);
		metadata.setModelId(modelId);

		String jobName =  commandLine.getOptionValue("jobName");
		metadata.setName(jobName);

		return metadata;
		
	}

	private static List<ETLFileOldDTO> handleFileOptions(String[] etlFileOptionValues) {
		List<ETLFileOldDTO> etlFiles = new ArrayList<>();

		if (etlFileOptionValues != null) {
			
			for(String etlFileOption : etlFileOptionValues)  {
				try {
					ETLFileOldDTO etlFile = parseETLFileArgs(etlFileOption);
					String key = "file" + (FIRST_FILE_INDEX++);			
					etlFile.setMimePart(key);
					etlFiles.add(etlFile);
				}
				catch (IllegalArgumentException e) {
					System.err.println( "Error: The value \""+etlFileOption+"\" for option --file is invalid.  " + e.getMessage());
					System.err.println( "\nPlease specify the filename followed by the file type, table name, and optional arguments."
							+ "\n Example: --file intersections.csv;intersections"
							+ "\n Example: --file arbitrary.csv;user_defined;mytable;bulkInsert=true");
					System.err.println( "\nOr specify options in any order using key-value pairs."
							+ "\n Example: --file \"file=arbitrary.csv; type=user_defined; table=mytable; format=CSV; bulkInsert=true\"");
					System.exit(1);
				}
			}
		}
		return etlFiles;
	}

	private static ETLFileOldDTO parseETLFileArgs(String etlFileOption) {
		ETLFileOldDTO etlFile = new ETLFileOldDTO();

		String[] optionFields = etlFileOption.split(";");

		List<String> unqualifiedFields = new ArrayList<>();

		for (String field : optionFields) {
			String[] parts = field.split("=", 2);

			if (parts.length == 1) {
				unqualifiedFields.add(field.trim());
			}
			else if (parts.length == 2) {
				String key = parts[0].trim();
				String value = parts[1].trim();

				switch (key) {
				case "bulkInsert":
					etlFile.setBulkInsert(Boolean.valueOf(value));
					break;
				case "file":
					etlFile.setFilename(value);
					break;
				case "format":
					etlFile.setFileFormat(FileFormat.valueOf(FileFormat.class, value));
					break;
				case "table":
					etlFile.setTableName(value);
					break;
				case "type":
					try {
						etlFile.setFileType(DataType.valueOf(DataType.class, value));
					} catch (IllegalArgumentException e) {
						throw new IllegalArgumentException("The ETL file type \""+value+"\" does not exist. The supported filetypes are ["+ETLFileOldDTO.SUPPORTED_FILETYPES_LIST+"]");
					}
					break;
				case "clearSlices":
					if (value != null) {
						etlFile.setClearSlicesExpressions(Arrays.asList(value.split(",")));
					}
					break;
				case "clearSlicesByDimNums":
					if (value != null) {
						etlFile.setClearSlicesDimensions(parseClearSlicesByDimNumsArgs(value));
					}
					break;

				case "clearSlicesByColumns":
					if (value != null && !value.isEmpty()) {
						// Allow users to use RFC-4180 for columns
						try {
							CSVParser parser = CSVParser.parse(value, CSVFormat.RFC4180);
							Iterator<CSVRecord> itr = parser.iterator();
							if (itr.hasNext()) {
								CSVRecord record = itr.next();
								List<String> columnNames = new ArrayList<>();
								for (final String currentValue : record) {
									columnNames.add(currentValue);
								}
								if (!columnNames.isEmpty()) {
									etlFile.setClearSlicesColumns(columnNames);
								}
							}
						} catch (IOException e) {
							throw new IllegalArgumentException("Invalid input for clearSlicesByColumns: \"" + value + "\"", e);
						}

					}
					break;
				case "encoding":
					if(value != null) {
						etlFile.setFileEncoding(value);
					}
					break;
				default:
					throw new IllegalArgumentException("Unsupported key " + key);
				}
			}
			else {
				throw new IllegalArgumentException("The field "+ field +" contained "+ parts.length +" parts.");
			}
		}

		if (unqualifiedFields.size() > 0) {
			String value = unqualifiedFields.get(0);
			if (etlFile.getFilename() != null) {
				System.out.println("Warning: overriding file="+ etlFile.getFilename() +" with "+ value);
			}
			etlFile.setFilename(value);
		}

		if (unqualifiedFields.size() > 1) {
			String value = unqualifiedFields.get(1);
			if (etlFile.getFileType() != null) {
				System.out.println("Warning: overriding type="+ etlFile.getFileType() +" with "+ value);
			}
			try {
				etlFile.setFileType(DataType.valueOf(DataType.class, value));
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException("The ETL file type \""+value+"\" does not exist. The supported filetypes are ["+ETLFileOldDTO.SUPPORTED_FILETYPES_LIST+"]");
			}
		}

		if (unqualifiedFields.size() > 2) {
			String value = unqualifiedFields.get(2);
			if (etlFile.getTableName() != null) {
				System.out.println("Warning: overriding table="+ etlFile.getTableName() +" with "+ value);
			}
			etlFile.setTableName(value);
		}

		if (etlFile.getFilename() == null) {
			throw new IllegalArgumentException("File name is required.");
		}

		if (etlFile.getFileType() == DataType.user_defined && etlFile.getTableName() == null) {
			throw new IllegalArgumentException("Table name is required for user-defined type.");
		}

		if (etlFile.getClearSlicesExpressions() != null && etlFile.getFileType() != DataType.intersections) {
			throw new IllegalArgumentException("The types supported for the clearSlices option are: {intersections}");
		}

		if (etlFile.getClearSlicesDimensions() != null && etlFile.getFileType() != DataType.intersections) {
			throw new IllegalArgumentException("The types supported for the clearSlicesByDimNums option are: {intersections}");
		}

		return etlFile;
	}

	private static Set<Integer> parseClearSlicesByDimNumsArgs(String clearSlicesByDimNumsOption) {
		if (clearSlicesByDimNumsOption != null && !clearSlicesByDimNumsOption.isEmpty()) {
			List<String> dimNumbers = Arrays.asList(clearSlicesByDimNumsOption.split(","));
			Set<Integer> clearSlicesDimNums = new HashSet<Integer>(dimNumbers.size());
				for (String num: dimNumbers) {
					clearSlicesDimNums.add(Integer.parseInt(num.trim()));
				}
			return clearSlicesDimNums;
		}
		return null;
	}
	
	private static ETLMetadataDTO fillTemplateMetadata(CommandLine commandLine, ETLTemplateDTO template) {
		ETLMetadataDTO metadata = template.getMetadata();
		List<ETLFileImportStepDTO> fileSteps = metadata.getAllFileSteps();
		String[] fileOptions = commandLine.getOptionValues("file") == null? new String[0] : commandLine.getOptionValues("file");
		if(fileSteps.size() != fileOptions.length) {
			System.err.println("ETL Template (" +template.getId()+") requires " + fileSteps.size() + " file(s).");
			System.err.println(fileOptions.length + " files were provided as arguments.");
			System.err.println("Please provide the correct number of input files to run template.");
			System.exit(1);
		}
		List<ETLFileOldDTO> files = handleFileOptions(fileOptions);
		for(int i = 0; i < fileSteps.size(); i++) {
			ETLFileImportStepDTO step = fileSteps.get(i);
			ETLFileOldDTO file = files.get(i);
			// Check that the fileType is not empty and matches the template step file type except in the case
			// of ETLFileToVenaTableSteps (in which the dataType and fileType are null)
			if (!(step instanceof ETLFileToVenaTableStepDTO)) {
				if (file.getFileType() == null || !step.getDataType().equals(file.getFileType())) {
					System.err.println("Error: File step type must match input file type");
					System.err.println("  step " + i + " type: " + step.getDataType() + ", file type: " + file.getFileType());
					System.exit(1);
				}
			}
			step.setCompressed(true); // ETL Tool compresses all file uploads.
			step.setFileEncoding(file.getFileEncoding());
			step.setFileFormat(file.getFileFormat());
			step.setFileName(file.getFilename());
			step.setMimePart(file.getMimePart());
		}
		return metadata;
	}

	private static void validateNonEmptyFileType(ETLFileOldDTO etlFile) {
		if (etlFile.getFileType() == null) {
			System.err.println( "Error: One of the ETL files does not have the required type field.");
			System.exit(1);
		}
	}

	private static void validateFileToVenaStep(ETLFileOldDTO etlFile) {
		if (etlFile.getTableName() == null || etlFile.getTableName().isEmpty()) {
			System.err.println( "Error: The table name must be specified for the Vena Table step.");
			System.exit(1);
		}
	}
	
}
