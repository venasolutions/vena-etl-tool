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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import javax.ws.rs.core.MediaType;

import org.vena.api.customer.authentication.APILoginResult;
import org.vena.api.etl.ETLFileImportStep;
import org.vena.api.etl.ETLFileOld;
import org.vena.api.etl.ETLJob.Phase;
import org.vena.api.etl.ETLMetadata;
import org.vena.api.etl.ETLStep;
import org.vena.api.etl.ETLStep.DataType;
import org.vena.api.etl.ETLCubeToStageStep;
import org.vena.api.etl.ETLTableStatus;
import org.vena.api.etl.QueryDTO;
import org.vena.api.etl.QueryDTO.Destination;
import org.vena.api.etl.QueryExpressionDTO;
import org.vena.etltool.entities.CreateModelRequestDTO;
import org.vena.etltool.entities.ETLJobDTO;
import org.vena.etltool.entities.ModelResponseDTO;
import org.vena.etltool.util.TwoTuple;
import org.vena.id.Id;

import com.fasterxml.jackson.databind.DeserializationFeature;
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

public class ETLClient {
	private static final int POLL_INTERVAL = 5000;
	
	protected Integer port = null;
	protected String host = "vena.io";
	protected String apiUser;
	protected String apiKey;
	public String username;
	public String password;
	public boolean needsLogin = false;
	public Id modelId;
	public String protocol = "https";
	public String location;
	public String templateId;
	public boolean validationRequested = false;
	public boolean pollingRequested = false;
	public boolean waitFully = false;
	public boolean verbose;

	public ETLClient() {
	}
	 
	public ETLJobDTO uploadETL(ETLMetadata metadata)
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
			
			WebResource webResource = buildWebResource(resource, parameters, true);
			
			FormDataMultiPart form = new FormDataMultiPart();

			ObjectMapper objectMapper = new ObjectMapper();
			
			byte[] metadataBytes = objectMapper.writeValueAsBytes(metadata);

			FormDataBodyPart metadataPart = new FormDataBodyPart("metadata",new ByteArrayInputStream(metadataBytes), MediaType.APPLICATION_JSON_TYPE);
			form.bodyPart(metadataPart);
			
			for (ETLStep step :  metadata.getSteps()) {
				if (step instanceof ETLFileImportStep)
				{
					String mimePart = ((ETLFileImportStep)step).getMimePart();
					String fileName = ((ETLFileImportStep)step).getFileName();
				
					FormDataBodyPart filePart = new FormDataBodyPart(mimePart, new FileInputStream(fileName), MediaType.APPLICATION_OCTET_STREAM_TYPE);
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
	
	private  String buildURI(String path) {
		return buildURI(path, Collections.<TwoTuple<String, String>> emptyList());
	}
	
	private  String buildURI(String path, Iterable<TwoTuple<String, String>> parameters)
	{
		StringBuilder urlBuf = new StringBuilder();
		
		if (location != null) {
			urlBuf.append(location);
		} else {
			urlBuf.append(protocol).append("://");
			urlBuf.append(host);
		}
		if (port != null) {
			urlBuf.append(":").append(port);
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

	private WebResource buildWebResource(String path) {
		return buildWebResource(path, Collections.<TwoTuple<String, String>> emptyList(), false);
	}

	private WebResource buildWebResource(String path, Iterable<TwoTuple<String, String>> parameters) {
		return buildWebResource(path, parameters, false);
	}

	private WebResource buildWebResource(String path, Iterable<TwoTuple<String, String>> parameters, boolean chunked) {

		ClientConfig jerseyClientConfig = new DefaultClientConfig();
		jerseyClientConfig.getClasses().add(MultiPartWriter.class);
		
		Client client = Client.create(jerseyClientConfig);

		if (chunked)
			client.setChunkedEncodingSize(8192);

		client.addFilter(new HTTPBasicAuthFilter(apiUser, apiKey));

		String uri = buildURI(path, parameters);
		
		if( verbose )
			System.err.println("Calling " + uri);

		WebResource webResource = client.resource(uri);

		webResource.accept("application/json");
		
		return webResource;
	}

	public void login()
	{
		Client client = Client.create();

		client.addFilter(new HTTPBasicAuthFilter(username, password));

		String uri = buildURI("/login");
		
		if( verbose )
			System.err.println("Calling " + uri);
		
		WebResource webResource = client.resource(uri);

		webResource.accept("application/json");


		ClientResponse response = webResource.post(ClientResponse.class);

		if (response.getStatus() != 200) {
			handleErrorResponse(response, "Login failed.");
		}

		APILoginResult result = getEntity(response, APILoginResult.class);

		this.apiKey = result.getApiKey();
		this.apiUser = result.getApiUser();
		this.location = result.getLocation();
	}
	
	//FIMXE - there is some code duplication between login() and this method that should be refactored out.
	public ModelResponseDTO createModel(String modelName)  {

		WebResource webResource = buildWebResource("/api/models");

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

		WebResource webResource = buildWebResource("/api/models");

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
		WebResource webResource = buildWebResource(getETLBasePath() + "/jobs/" + idString);


		ClientResponse response = webResource.get(ClientResponse.class);
		
		if (response.getStatus() != 200) {
			handleErrorResponse(response, "Unable to get job status.");
		}

		ETLJobDTO result = getEntity(response, ETLJobDTO.class);
		
		return result;
	}

	public static String requestVersionInfo() {
		Properties props = new Properties();
		StringBuilder buf = new StringBuilder();

		try {
			props.load(ETLClient.class
					.getResourceAsStream("/global.properties"));

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
	
	public ETLJobDTO setJobError(String idString, String errMsg) throws UnsupportedEncodingException
	{
		List<TwoTuple<String, String>> params = new ArrayList<>();
		params.add(new TwoTuple<String, String>("message", errMsg));

		WebResource webResource = buildWebResource(getETLBasePath() + "/jobs/"+idString + "/setError", params);

		ClientResponse response = webResource.get(ClientResponse.class);

		if (response.getStatus() != 200) {
			handleErrorResponse(response, "Unable to set job error.");
		}

		ETLJobDTO result = getEntity(response, ETLJobDTO.class);

		return result;
	}

	public ETLJobDTO sendTransformComplete(String idString)
	{
		WebResource webResource = buildWebResource(getETLBasePath() + "/jobs/"+idString + "/transformComplete");

		ClientResponse response = webResource.get(ClientResponse.class);

		if (response.getStatus() != 200) {
			handleErrorResponse(response, "'transformComplete' request failed.");
		}

		ETLJobDTO etlJob = getEntity(response, ETLJobDTO.class);
		

		return etlJob;
	}

	public ETLJobDTO sendCancel(String idString)
	{
		WebResource webResource = buildWebResource(getETLBasePath() + "/jobs/"+idString + "/cancel");

		ClientResponse response = webResource.get(ClientResponse.class);

		if (response.getStatus() != 200) {
			handleErrorResponse(response, "Cancel request failed.");
		}

		ETLJobDTO etlJob = getEntity(response, ETLJobDTO.class);
		
		return etlJob;
	}	

	public InputStream sendExport(DataType type, boolean toFile, String tableName, String whereClause, String queryExpr, boolean showHeaders) {
		
		String typePath = null;

		if (whereClause != null) {
			switch (type) {
			case attributes:
				typePath = "attributes";
				break;
			case hierarchy:
				typePath = "hierarchies";
				break;
			case intersections:
				typePath = "intersections";
				break;
			case lids:
				typePath = "lids";
				break;
			default:
				System.err.println("Type \""+type+"\" not supported for export.");
			}
		}

		else if (queryExpr != null) {
			switch (type) {
			case attributes:
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
			default:
				System.err.println("Type \""+type+"\" not supported for export.");
			}
		}

		if (typePath == null) {
			System.exit(1);
			return null;
		}

		WebResource webResource = buildWebResource(getETLBasePath() + "/query/" + typePath);

		QueryDTO query = new QueryDTO();
		if (!toFile) {
			query.setDestination(Destination.ToStaging);
			query.setTableName(tableName);
		} else {
			query.setDestination(Destination.ToCSV);
		}
		if (whereClause != null) {
			query.setQueryString(whereClause);
		} else if (queryExpr != null) {
			query.setQueryString(queryExpr);
		}
		query.setShowHeaders(showHeaders);

		ClientResponse response = webResource.type(MediaType.APPLICATION_JSON_TYPE).post(ClientResponse.class, query);

		if ((response.getStatus() != 204) && (response.getStatus() != 200)) {
			handleErrorResponse(response, "Request to export failed.");
		}

		if (toFile) return response.getEntityInputStream();
		else return null;
	}

	public void sendDelete(DataType type, String expression) {

		String typePath;

		switch (type) {
		case intersections:
			typePath = "intersections";
			break;
		default:
			System.err.println("Type \""+type+"\" not supported for delete.");
			return;
		}

		WebResource webResource = buildWebResource(getETLBasePath() + "/delete/" + typePath);

		QueryExpressionDTO queryExpr = new QueryExpressionDTO();
		queryExpr.setExpression(expression);

		ClientResponse response = webResource.type(MediaType.APPLICATION_JSON_TYPE).post(ClientResponse.class, queryExpr);
		
		if ((response.getStatus() != 204) && (response.getStatus() != 200)) {
			handleErrorResponse(response, "Request to delete failed.");
		}

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
		
		ETLMetadata metadata = etlJob.getMetadata();
		
    	System.out.println();
		System.out.println("  Job Id: " + etlJob.getId());
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
		System.out.println("  Load Type: " + (metadata.getLoadType() == null ? "-" :
			ETLMetadata.loadTypeToString(metadata.getLoadType())));
		
		if (metadata.getSteps() != null) {
			System.out.println("  Steps:");
			for (ETLStep step : metadata.getSteps()) {
				System.out.println("   - " + step.getStepNumber() + "- "
						+ step.getName() + " (STATUS " + step.getStatus()
						+ ") " + " (" + step.getPercentDone() + "% DONE)");
			}
		}

		if (metadata.getTables() != null) {
			System.out.println("  Tables:");
			for (ETLTableStatus table : metadata.getTables().values()) {
				System.out.println("   - " + table.getTableName() + ":  " + 
						(table.getDone() ? "Done!":"Not done") + " (processed " + table.getRowsProcessed() + " rows)");
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