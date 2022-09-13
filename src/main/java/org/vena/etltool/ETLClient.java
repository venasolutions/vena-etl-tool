package org.vena.etltool;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.zip.DeflaterInputStream;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import org.vena.etltool.entities.CreateModelRequestDTO;
import org.vena.etltool.entities.ETLCalculationDeployStepDTO;
import org.vena.etltool.entities.ETLCubeToStageStepDTO;
import org.vena.etltool.entities.ETLFileImportStepDTO;
import org.vena.etltool.entities.ETLFileImportStepDTO.FileFormat;
import org.vena.etltool.entities.ETLJobDTO;
import org.vena.etltool.entities.ETLJobDTO.Phase;
import org.vena.etltool.entities.ETLMetadataDTO;
import org.vena.etltool.entities.ETLStageToCubeStepDTO;
import org.vena.etltool.entities.ETLStepDTO;
import org.vena.etltool.entities.ETLStepDTO.DataType;
import org.vena.etltool.entities.ETLStepDTO.Status;
import org.vena.etltool.entities.ETLTemplateDTO;
import org.vena.etltool.entities.ETLVersioningStepDTO;
import org.vena.etltool.entities.Id;
import org.vena.etltool.entities.LoginResultDTO;
import org.vena.etltool.entities.ModelResponseDTO;
import org.vena.etltool.entities.QueryDTO;
import org.vena.etltool.entities.QueryDTO.Destination;
import org.vena.etltool.util.TwoTuple;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource.Builder;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.multipart.FormDataBodyPart;
import com.sun.jersey.multipart.FormDataMultiPart;
import com.sun.jersey.multipart.impl.MultiPartWriter;

public class ETLClient {
	private static final int POLL_INTERVAL = 5000;
	public static final String DEFAULT_HOST = "vena.io";
	public static final List<String> LOGIN_HOSTS = Arrays.asList("ca3.vena.io", "eu1.vena.io", "us1.vena.io", "us2.vena.io", "us3.vena.io");
	
	protected Integer port = null;
	protected String host = DEFAULT_HOST;
	protected String apiUser;
	protected String apiKey;
	public String username;
	public String password;
	public Id modelId;
	public String protocol = "https";
	public String location;
	public String templateId;
	public boolean validationRequested = false;
	public boolean pollingRequested = false;
	public boolean waitFully = false;
	public boolean verbose;

	private String userAgent;

	private JerseyClientFactory clientFactory;
	private Client uploadClient;
	private Client apiClient;

	public ETLClient(JerseyClientFactory clientFactory) {
		this.clientFactory = clientFactory;
	}

	public ETLTemplateDTO getETLTemplate() {
		// Get the ETL template from the server.
		try {
			String resource = getETLBasePath() + "/templates/" +templateId;
			Builder webResource = buildWebResource(resource);
			ClientResponse response = webResource.type(MediaType.APPLICATION_JSON_TYPE).get(ClientResponse.class);
			switch(response.getStatus()) {
			case 200:
				return getEntity(response, ETLTemplateDTO.class);
			default:
				handleErrorResponse(response, "Could not retrieve ETL Template.");
			}
			
		} catch (Exception e) {

			e.printStackTrace();
		}
		System.exit(1);
		return null; // should never reach here
	}

	public ETLJobDTO uploadETL(ETLMetadataDTO metadata)
	{
		try {
			String resource;

			if( !validationRequested ) {
				resource = getETLBasePath() + "/upload";
			}
			else {
				resource = getETLBasePath() + "/validate";
			}
			
			List<TwoTuple<String, String>> parameters = new ArrayList<>();
			if( templateId != null ) {
				parameters.add(new TwoTuple<String, String>("templateId", templateId));
			}
			
			Builder webResource = buildWebResource(resource, parameters, true);
			
			FormDataMultiPart form = new FormDataMultiPart();

			ObjectMapper objectMapper = new ObjectMapper();
			
			byte[] metadataBytes = objectMapper.writeValueAsBytes(metadata);

			FormDataBodyPart metadataPart = new FormDataBodyPart("metadata",new ByteArrayInputStream(metadataBytes), MediaType.APPLICATION_JSON_TYPE);
			form.bodyPart(metadataPart);
			for (ETLStepDTO step :  metadata.getSteps()) {
				if (step instanceof ETLFileImportStepDTO) {
					String mimePart = ((ETLFileImportStepDTO) step).getMimePart();
					String fileName = ((ETLFileImportStepDTO) step).getFileName();
					InputStream stream = new DeflaterInputStream(new FileInputStream(fileName));
					FormDataBodyPart filePart = new FormDataBodyPart(mimePart, stream, MediaType.APPLICATION_OCTET_STREAM_TYPE);
					form.bodyPart(filePart);
				}
			}
						
			ClientResponse response = webResource.type(MediaType.MULTIPART_FORM_DATA_TYPE).post(ClientResponse.class, form);			
			
			switch( response.getStatus()) {
			
			case 200:
				ETLJobDTO etlJob = getEntity(response, ETLJobDTO.class);
				return etlJob;
			default:
				handleErrorResponse(response, "Unable to submit job.");
			}

		} catch (Exception e) {

			e.printStackTrace();
		}
		System.exit(1);
		return null; // should never reach here
	}

	void pollTillJobComplete(Id jobId, boolean waitFully) {
		while( true) {
			ETLJobDTO etlJob = requestJobStatus(jobId);

			if( ! isJobStillRunning(etlJob) || (! waitFully && isJobInStaging(etlJob)))  {

				printJobStatus(etlJob);
				System.out.println();
				
				if (etlJob.isError() || etlJob.isCancelRequested()) {
					System.out.println("The job stopped due to error or was cancelled.");
					System.exit(1);
				}
				break;
			}
			else {
				try {
					Thread.sleep(POLL_INTERVAL);
				}
				catch(InterruptedException intEx) {
				}
			}
		}
	}
	
	private boolean isJobStillRunning(ETLJobDTO etlJob) {
		if (etlJob.isError()) 
			return false;
		else if (etlJob.isCancelRequested()) 
			return false;
		else if (etlJob.getPhase() == Phase.COMPLETE) 
			return false;
		else 
			return true;
	}

	private boolean isJobInStaging(ETLJobDTO etlJob) {
		return (etlJob.getPhase() == Phase.IN_STAGING);
	}

	private String getETLBasePath() {
		if (modelId == null) {
			return "/api/etl";
		}
		return "/api/models/" + modelId + "/etl";
	}

	private String buildURI(String path) {
		return buildURIForHost(host, path);
	}
	
	private String buildURI(String path, Iterable<TwoTuple<String, String>> parameters)
	{
		return buildURIForHost(host, path, parameters);
	}

	private String buildURIForHost(String host, String path) {
		return buildURIForHost(host, path, Collections.<TwoTuple<String, String>> emptyList());
	}

	private String buildURIForHost(String host, String path, Iterable<TwoTuple<String, String>> parameters)
	{
		StringBuilder urlBuf = new StringBuilder();
		
		if (location != null) {
			urlBuf.append(location);
		} else {
			urlBuf.append(protocol).append("://");
			urlBuf.append(host);
			if (port != null) {
				urlBuf.append(":").append(port);
			}
		}

		urlBuf.append(path);
		
		Iterator<TwoTuple<String, String>> it = parameters.iterator();
		
		StringBuilder parameterBuf = new StringBuilder();
		
		if(it.hasNext())
			parameterBuf.append("?");
		
		while(it.hasNext()) {
			TwoTuple<String, String> parameter = it.next();
			
			try {
				parameterBuf.append(URLEncoder.encode(parameter.getO1(), "UTF-8"))
					.append("=").append(URLEncoder.encode(parameter.getO2(), "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
			
			if(it.hasNext())
				parameterBuf.append("&");
		}

		urlBuf.append(parameterBuf);
		
		return urlBuf.toString();
	}

	private Client getUploadClient() {
		if (uploadClient == null) {
			ClientConfig jerseyClientConfig = new DefaultClientConfig();
			jerseyClientConfig.getClasses().add(MultiPartWriter.class);

			uploadClient = clientFactory.create(jerseyClientConfig);
			uploadClient.setChunkedEncodingSize(8192);
			uploadClient.addFilter(new HTTPBasicAuthFilter(apiUser, apiKey));
		}
		return uploadClient;
	}

	private Client getAPIClient() {
		if (apiClient == null) {
			apiClient = clientFactory.create();
			apiClient.addFilter(new HTTPBasicAuthFilter(apiUser, apiKey));
		}
		return apiClient;
	}

	private Builder buildWebResource(String path) {
		return buildWebResource(path, Collections.<TwoTuple<String, String>> emptyList(), false);
	}

	private Builder buildWebResource(String path, Iterable<TwoTuple<String, String>> parameters) {
		return buildWebResource(path, parameters, false);
	}

	private Builder buildWebResource(String path, Iterable<TwoTuple<String, String>> parameters, boolean chunked) {

		Client client = chunked ? getUploadClient() : getAPIClient();
		String uri;
		if(parameters == null || !parameters.iterator().hasNext() ) {
			uri = buildURI(path);
		} else {
			uri = buildURI(path, parameters);
		}
		
		if( verbose )
			System.err.println("Calling " + uri);

		return client.resource(uri)
				.accept("application/json")
				.header(HttpHeaders.USER_AGENT, getUserAgent());
	}

	private Builder buildLoginResource(Client client, String host) {
		String uri = buildURIForHost(host, "/login");

		if( verbose )
			System.err.println("Calling " + uri);

		return client.resource(uri)
				.accept("application/json")
				.header(HttpHeaders.USER_AGENT, getUserAgent());
	}

	public void login()
	{
		Client client = clientFactory.create();

		client.addFilter(new HTTPBasicAuthFilter(username, password));

		Builder webResource = buildLoginResource(client, host);

		ClientResponse response = webResource.post(ClientResponse.class);

		if (host.equals(DEFAULT_HOST)) {
			// Workaround for vena.io resolving to a bad DC. Only needed for logins on default host.
			int retryCount = LOGIN_HOSTS.size();

			// Start at a random host so that we don't always spam the same DC when there is an outage.
			int index = new Random().nextInt(LOGIN_HOSTS.size());

			// We had a case where nginx returned 404 when all mt-servers in a DC were unavailable.
			while (retryCount > 0 && ( response.getStatus() == 404 || response.getStatus() >= 500 )) {
				String nextHost = LOGIN_HOSTS.get(index);
				index = (index + 1) % LOGIN_HOSTS.size();
				webResource = buildLoginResource(client, nextHost);
				response = webResource.post(ClientResponse.class);
				retryCount--;
			}
		}

		if (response.getStatus() != 200) {
			handleErrorResponse(response, "Login failed.");
		}

		LoginResultDTO result = getEntity(response, LoginResultDTO.class);

		this.apiKey = result.getApiKey();
		this.apiUser = result.getApiUser();
		this.location = result.getLocation();
	}

	//FIMXE - there is some code duplication between login() and this method that should be refactored out.
	public ModelResponseDTO createModel(String modelName)  {
		
		Builder webResource = buildWebResource("/api/models");

		CreateModelRequestDTO createModelDTO = new CreateModelRequestDTO();

		createModelDTO.setName(modelName);
		createModelDTO.setDesc(modelName);

		ClientResponse response = webResource.type(MediaType.APPLICATION_JSON_TYPE).post(ClientResponse.class, createModelDTO);

		if (response.getStatus() != 200) {
			handleErrorResponse(response, "Create model failed.");
		}

		ModelResponseDTO result = getEntity(response, ModelResponseDTO.class);

		this.modelId = result.getId();
		
		return result;
	}
	
	public ModelResponseDTO lookupModel(String modelName) {

		Builder webResource = buildWebResource("/api/models");

		CreateModelRequestDTO createModelDTO = new CreateModelRequestDTO();

		createModelDTO.setName(modelName);
		createModelDTO.setDesc(modelName);

		ClientResponse response = webResource.type(MediaType.APPLICATION_JSON_TYPE).get(ClientResponse.class);

		if (response.getStatus() != 200) {
			handleErrorResponse(response, "Lookup model failed.");
		}

		List<ModelResponseDTO> results = getListOfEntity(response, ModelResponseDTO.class);

		for(ModelResponseDTO model : results)  {
			if(modelName.equals(model.getName()))
				return model;
		}
		
		return null;
	}
	
	public ETLJobDTO requestJobStatus(Id etlJobId)
	{
		return requestJobStatus(etlJobId.toString());
	}
	
	public ETLJobDTO requestJobStatus(String idString)
	{
		Builder webResource = buildWebResource(getETLBasePath() + "/jobs/" + idString);


		ClientResponse response = webResource.get(ClientResponse.class);
		
		if (response.getStatus() != 200) {
			handleErrorResponse(response, "Unable to get job status.");
		}

		ETLJobDTO result = getEntity(response, ETLJobDTO.class);
		
		return result;
	}
	
	private String getUserAgent() {
		if (userAgent != null) {
			return userAgent;
		}
		
		try {
			Properties props = getGlobalProperties();
			//don't want to have to read from disk every time
			userAgent = props.getProperty("artifactId") + "/" + props.getProperty("version") + "/" + props.getProperty("git.commit.id");
			return userAgent;
		} catch (IOException e1) {
			e1.printStackTrace();
			return "cmdline-etl-tool";
		}
	}

	public static String requestVersionInfo() {
		StringBuilder buf = new StringBuilder();

		try {
			Properties props = getGlobalProperties();

			String[] keys = new String[] { "artifactId", "version",
					"git.branch", "git.commit.id", "git.commit.id.describe", 
					"git.commit.time", "git.build.time" };

			for (String key : keys) {
				buf.append(key).append(": ");
				buf.append(props.getProperty(key)).append("\n");
			}

			return buf.toString();

		} catch (Exception e) {
			e.printStackTrace();
			return "Error: Could not extract any git information.";
		}
	}
	
	private static Properties getGlobalProperties() throws IOException {
		try (InputStream is = ETLClient.class.getResourceAsStream("/global.properties")) {
			Properties props = new Properties();
			//WARNING: props.load() doesn't close the input stream!
			props.load(is);
			return props;
		}
	}
	
	public ETLJobDTO setJobError(String idString, String errMsg) throws UnsupportedEncodingException
	{
		List<TwoTuple<String, String>> params = new ArrayList<>();
		params.add(new TwoTuple<String, String>("message", errMsg));

		Builder webResource = buildWebResource(getETLBasePath() + "/jobs/"+idString + "/setError", params);

		ClientResponse response = webResource.get(ClientResponse.class);

		if (response.getStatus() != 200) {
			handleErrorResponse(response, "Unable to set job error.");
		}

		ETLJobDTO result = getEntity(response, ETLJobDTO.class);

		return result;
	}

	public ETLJobDTO sendTransformComplete(String idString)
	{
		Builder webResource = buildWebResource(getETLBasePath() + "/jobs/"+idString + "/transformComplete");

		ClientResponse response = webResource.get(ClientResponse.class);

		if (response.getStatus() != 200) {
			handleErrorResponse(response, "'transformComplete' request failed.");
		}

		ETLJobDTO etlJob = getEntity(response, ETLJobDTO.class);
		

		return etlJob;
	}

	public ETLJobDTO sendCancel(String idString)
	{
		Builder webResource = buildWebResource(getETLBasePath() + "/jobs/"+idString + "/cancel");

		ClientResponse response = webResource.get(ClientResponse.class);

		if (response.getStatus() != 200) {
			handleErrorResponse(response, "Cancel request failed.");
		}

		ETLJobDTO etlJob = getEntity(response, ETLJobDTO.class);
		
		return etlJob;
	}	

	public InputStream sendExport(DataType type, String tableFromName, String tableToName, String whereClause, String queryExpr, boolean showHeaders, FileFormat format){

		String typePath = null;

		if (whereClause != null) {
			switch (type) {
			case attributes:
				typePath = "attributes";
				break;
			case hierarchy:
				typePath = "hierarchies";
				break;
			case user_defined:
				typePath = "staging";
				break;
			case intersections:
			case lids:
				System.err.println("Type \""+type+"\" doesn't support where clause. Use query expression instead.");
				break;
			default:
				System.err.println("Type \""+type+"\" not supported for export.");
			}
		}

		else if (queryExpr != null) {
			switch (type) {
			case attributes:
			case user_defined:
			case hierarchy:
				System.err.println("Type \""+type+"\" doesn't support query expression. Use where clause instead.");
				break;
			case lids:
				typePath = "lids2";
				break;
			case intersections:
				typePath = "intersections2";
				break;
			default:
				System.err.println("Type \""+type+"\" not supported for export.");
			}
		}

		else { // both whereClause and queryExpr are null
			switch (type) {
			case attributes:
				typePath = "attributes";
				break;
			case hierarchy:
				typePath = "hierarchies";
				break;
			case intersections:
				typePath = "intersections2";
				break;
			case lids:
				typePath = "lids2";
				break;
			case user_defined:
				typePath = "staging";
				break;
			default:
				System.err.println("Type \""+type+"\" not supported for export.");
			}
		}

		if (typePath == null) {
			System.exit(1);
			return null;
		}

		Builder webResource = buildWebResource(getETLBasePath() + "/query/" + typePath);
		QueryDTO query = new QueryDTO();

		query.setDestination(Destination.ToCSV);
		query.setTableName(tableFromName);

		if (whereClause != null) {
			query.setQueryString(whereClause);
		} else if (queryExpr != null) {
			query.setQueryString(queryExpr);
		}
		query.setShowHeaders(showHeaders);
		query.setFormat(format);

		ClientResponse response = webResource.type(MediaType.APPLICATION_JSON_TYPE).post(ClientResponse.class, query);
		if ((response.getStatus() != 204) && (response.getStatus() != 200)) {
			handleErrorResponse(response, "Request to export failed.");
		}
		return response.getEntityInputStream();
	}

	private void handleErrorResponse(ClientResponse response, String message) {

		System.err.println("ERROR:");
		System.err.println(">>> " + response);
		BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntityInputStream()));
		String line;
		try {
			while ((line = reader.readLine()) != null) {
				System.err.println(">>> " + line);
			}
		} catch (IOException e) {
			System.err.println("An error occurred trying to read the response from the server.");
			e.printStackTrace(System.err);
		}
		
		System.err.println();

		switch( response.getStatus()) {
		case 200:
		case 204:
			System.err.println("No error.");
			break;
		case 401:
			System.err.println("Access denied.  Check your credentials and try again.");
			break;
		case 403:
			System.err.println("Permission denied. Login was successful, but your user does not have permission to perform this operation.");
			break;
		case 404:
			System.err.println("The path was incorrect. Usually this is an incorrect ID somewhere.");
			break;
		case 422:
			System.err.println("The server rejected the input. See response message for more info.");
			break;
		case 500:
			System.err.println("Server error. Contact your administrator.");
			break;
		default:
			System.err.println("Unknown error.");
			break;
		}

		if (message != null) System.out.println(message);
		System.exit(1);
	}

	static void printJobStatus(ETLJobDTO etlJob) {
		
		ETLMetadataDTO metadata = etlJob.getMetadata();
		
    	System.out.println();
		System.out.println("  Job Id: " + etlJob.getId());
		System.out.println("  Job Name: " + (etlJob.getMetadata().getName() == null ? "-" : etlJob.getMetadata().getName()));
		System.out.println("  Template Id: " + (etlJob.getTemplateId() == null ? "-" : etlJob.getTemplateId()));
		System.out.println("  Model id: " + metadata.getModelId());
		System.out.println("  Created: " + (etlJob.getCreatedDate() == null ? "-" : etlJob.getCreatedDate()));
		System.out.println("  Updated: " + (etlJob.getUpdatedDate() == null ? "-" : etlJob.getUpdatedDate()));

		String userString;
		if (etlJob.getUser() != null) {
			userString = etlJob.getUser().toString();
		} else if (etlJob.getUserId() != null) {
			userString = etlJob.getUserId().toString();
		} else {
			userString = "-";
		}
		System.out.println("  Created by user: " + userString);
		
		if (metadata.getSteps() != null) {
			System.out.println("  Steps:");
			for (ETLStepDTO step : metadata.getSteps()) {
				System.out.print("   - " + step.getStepNumber()
						+ ". " + step.getName()
						+ ": " + step.getStatus());
				if (step.getStatus() != Status.NOT_STARTED) {
					System.out.print(" (" + step.getPercentDone()+ "% Done)"
							+ ((step instanceof ETLFileImportStepDTO)? 
									" (Processed " + ((ETLFileImportStepDTO) step).getLinesProcessed() + " lines)": "")
							+ ((step instanceof ETLStageToCubeStepDTO)? 
									" (Processed " + ((ETLStageToCubeStepDTO) step).getRowsProcessed() + " rows)": "") 
							+ ((step instanceof ETLCubeToStageStepDTO)? 
									" (Exported " + ((ETLCubeToStageStepDTO) step).getRowsExported() + " rows)": "")
							+ ((step instanceof ETLVersioningStepDTO)? 
									" (Processed " + ((ETLVersioningStepDTO) step).getSourceIntersectionsProcessed() + " source intersections)": "")
							+ ((step instanceof ETLCalculationDeployStepDTO)? 
									" (Processed " + ((ETLCalculationDeployStepDTO) step).getSourceIntersectionsProcessed() + " source intersections)": "")
					);
				}
				if (step.getStatus() == Status.ERROR || step.getStatus() == Status.CANCELLED || step.getStatus() == Status.WAITING) {
					System.out.print(((step instanceof ETLFileImportStepDTO)? 
									" (Resume at " + ((ETLFileImportStepDTO) step).getResumeLine() + " lines)": "")
							+ ((step instanceof ETLStageToCubeStepDTO)? 
									" (Resume at " + ((ETLStageToCubeStepDTO) step).getResumeRow() + " rows)": ""));
				}
				System.out.println();
			}
		}

		if (etlJob.getErrorMessage() != null) {
			System.out.println("  Error Message: " + etlJob.getErrorMessage());
		}

		if (etlJob.getValidationResults() != null) {
			System.out.println("  Validation Results: " + etlJob.getValidationResults());
		}

		System.out.println("  Status: " + etlJob.getStatus());

	}

	private static <T> T getEntity(ClientResponse response, Class<T> type) {
		String rawJSONOutput = response.getEntity(String.class);

		ObjectMapper objectMapper = new ObjectMapper();

		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		T result;

		try {
			result = objectMapper.readValue(rawJSONOutput, type);

			return type.cast(result);
		} catch ( IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static <T> List<T> getListOfEntity(ClientResponse response, Class<T> type) {
		String rawJSONOutput = response.getEntity(String.class);

		ObjectMapper objectMapper = new ObjectMapper();

		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		List<T> result;

		try {
			result = objectMapper.readValue(rawJSONOutput, objectMapper.getTypeFactory().constructCollectionType(List.class, type));

			return (List<T>) result;
		} catch ( IOException e) {
			throw new RuntimeException(e);
		}
	}
}
