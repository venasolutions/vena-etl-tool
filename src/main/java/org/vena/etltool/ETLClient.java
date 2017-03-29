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
import java.util.Properties;
import java.util.zip.DeflaterInputStream;

import javax.ws.rs.core.MediaType;
import org.vena.etltool.entities.CreateModelRequestDTO;
import org.vena.etltool.entities.ETLCalculationDeployStepDTO;
import org.vena.etltool.entities.ETLCubeToStageStepDTO;
import org.vena.etltool.entities.ETLFileImportStepDTO;
import org.vena.etltool.entities.ETLJobDTO;
import org.vena.etltool.entities.ETLJobDTO.Phase;
import org.vena.etltool.entities.ETLMetadataDTO;
import org.vena.etltool.entities.ETLStageToCubeStepDTO;
import org.vena.etltool.entities.ETLStepDTO;
import org.vena.etltool.entities.ETLStepDTO.DataType;
import org.vena.etltool.entities.ETLStepDTO.Status;
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

	private Client uploadClient;
	private Client apiClient;

	public ETLClient() {
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
			
			WebResource webResource = buildWebResource(resource, parameters, true);
			
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

	private Client getUploadClient() {
		if (uploadClient == null) {
			ClientConfig jerseyClientConfig = new DefaultClientConfig();
			jerseyClientConfig.getClasses().add(MultiPartWriter.class);

			uploadClient = Client.create(jerseyClientConfig);
			uploadClient.setChunkedEncodingSize(8192);
			uploadClient.addFilter(new HTTPBasicAuthFilter(apiUser, apiKey));
		}
		return uploadClient;
	}

	private Client getAPIClient() {
		if (apiClient == null) {
			apiClient = Client.create();
			apiClient.addFilter(new HTTPBasicAuthFilter(apiUser, apiKey));
		}
		return apiClient;
	}

	private WebResource buildWebResource(String path) {
		return buildWebResource(path, Collections.<TwoTuple<String, String>> emptyList(), false);
	}

	private WebResource buildWebResource(String path, Iterable<TwoTuple<String, String>> parameters) {
		return buildWebResource(path, parameters, false);
	}

	private WebResource buildWebResource(String path, Iterable<TwoTuple<String, String>> parameters, boolean chunked) {

		Client client = chunked ? getUploadClient() : getAPIClient();
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

		int retryCount = 5;
		while (retryCount > 0 && response.getStatus() != 200 && response.getStatus() >= 500 ) {
			try {
				Thread.sleep(2000);
			}
			catch( InterruptedException e) {
				break;
			}
			response = webResource.post(ClientResponse.class);
			retryCount--;
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

	public InputStream sendExport(DataType type, String tableFromName, boolean toFile, String tableToName, String whereClause, String queryExpr, boolean showHeaders){
		
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

		WebResource webResource = buildWebResource(getETLBasePath() + "/query/" + typePath);
		QueryDTO query = new QueryDTO();
		if (!toFile) {
			query.setDestination(Destination.ToStaging);
			query.setTableName(tableToName);
		} else {
			query.setDestination(Destination.ToCSV);
			query.setTableName(tableFromName);
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
		System.out.println("  Load Type: " + (metadata.getLoadType() == null ? "-" :
			ETLMetadataDTO.loadTypeToString(metadata.getLoadType())));
		
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
