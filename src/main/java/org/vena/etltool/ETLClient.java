package org.vena.etltool;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import javax.ws.rs.core.MediaType;

import org.vena.api.customer.authentication.LoginResult;
import org.vena.api.etl.ETLFile;
import org.vena.api.etl.ETLJob;
import org.vena.api.etl.ETLMetadata;
import org.vena.api.etl.QueryDTO;
import org.vena.api.etl.QueryDTO.Destination;
import org.vena.etltool.entities.CreateModelRequestDTO;
import org.vena.etltool.entities.ModelResponseDTO;
import org.vena.etltool.util.TwoTuple;
import org.vena.id.Id;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.multipart.FormDataBodyPart;
import com.sun.jersey.multipart.FormDataMultiPart;
import com.sun.jersey.multipart.impl.MultiPartWriter;

public class ETLClient {
	protected int port = 8080;
	protected String host = "localhost";
	protected String apiUser;
	protected String apiKey;
	public String username;
	public String password;
	public boolean needsLogin = false;
	public Id modelId;
	public String protocol = "http";
	public String templateId;
	public boolean validationRequested = false;

	public ETLClient() {
	}
	 
	public void uploadETL(ETLMetadata metadata)
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
			
			WebResource webResource = buildWebResource(resource, parameters);
			
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
			
			System.out.println(">>> " + response);

			switch( response.getStatus()) {
			
			case 200:
				ETLJob output = response.getEntity(ETLJob.class);

				System.out.println("Job submitted. Your ETL Job Id is "+output.getId());
				
				break;
			case 401:
				System.err.println("Access denied.  Check your credentials and try again.");
				System.exit(2);
				break;
			case 403:
				System.err.println("Permission denied. Login was successful, but your user does not have permission to perform an ETL upload.");
				System.exit(2);
				break;
			default:
				System.err.println("Error"+response);
				System.exit(4);
			}

		} catch (Exception e) {

			e.printStackTrace();

		}
	}
	
	private String getETLBasePath() {
		if (modelId == null) {
			return "/api/etl";
		}
		return "/api/models/" + modelId + "/etl";
	}
	
	private  String buildURI(String path, Iterable<TwoTuple<String, String>> parameters)
	{
		StringBuilder urlBuf = new StringBuilder();
		
		urlBuf.append(protocol).append("://");
		urlBuf.append(host).append(":");
		urlBuf.append(port);
		
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

	private  String buildURI(String path)
	{
		return buildURI(path, Collections.<TwoTuple<String, String>> emptyList());
	}


	private WebResource buildWebResource(String path) {
		return buildWebResource(path, Collections.<TwoTuple<String, String>> emptyList());
	}

	private WebResource buildWebResource(String path, Iterable<TwoTuple<String, String>> parameters) {

		ClientConfig jerseyClientConfig = new DefaultClientConfig();
		jerseyClientConfig.getClasses().add(MultiPartWriter.class);
		
		Client client = Client.create(jerseyClientConfig);

		client.addFilter(new HTTPBasicAuthFilter(apiUser, apiKey));

		String uri = buildURI(path);
		System.out.println("Calling " + uri);

		WebResource webResource = client.resource(uri);

		webResource.accept("application/json");
		
		return webResource;
	}

	public void login()
	{
		Client client = Client.create();

		client.addFilter(new HTTPBasicAuthFilter(username, password));

		String uri = protocol+"://"+host+":"+port+"/login";
		System.out.println("Calling " + uri);
		
		WebResource webResource = client.resource(uri);

		webResource.accept("application/json");


		ClientResponse response = webResource.post(ClientResponse.class);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Login failed : HTTP error code : "+ response.getStatus());
		}

		LoginResult result = response.getEntity(LoginResult.class);

		this.apiKey = result.getApiKey();
		this.apiUser = result.getApiUser();
	}
	
	//FIMXE - there is some code duplication between login() and this method that should be refactored out.
	public ModelResponseDTO createModel(String modelName) {

		WebResource webResource = buildWebResource("/api/models");

		CreateModelRequestDTO createModelDTO = new CreateModelRequestDTO();

		createModelDTO.setName(modelName);
		createModelDTO.setDesc(modelName);

		ClientResponse response = webResource.type(MediaType.APPLICATION_JSON_TYPE).post(ClientResponse.class, createModelDTO);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Create model failed : HTTP error code : "+ response.getStatus());
		}

		ModelResponseDTO result = response.getEntity(ModelResponseDTO.class);

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
			throw new RuntimeException("Lookup model failed : HTTP error code : "+ response.getStatus());
		}

		List<ModelResponseDTO> results = response.getEntity(new GenericType<List<ModelResponseDTO>>(){});

		for(ModelResponseDTO model : results)  {
			if(modelName.equals(model.getName()))
				return model;
		}
		
		return null;
	}
	
	public ETLJob requestJobStatus(String idString)
	{
		WebResource webResource = buildWebResource(getETLBasePath() + "/jobs/" + idString);


		ClientResponse response = webResource.get(ClientResponse.class);
		
		System.out.println(">>> " + response);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Unable to get job status. : "+ response.getStatus());
		}

		ETLJob result = response.getEntity(ETLJob.class);

		return result;
	}

	public ETLJob setJobError(String idString, String errMsg) throws UnsupportedEncodingException
	{
		List<TwoTuple<String, String>> params = new ArrayList<>();
		params.add(new TwoTuple<String, String>("message", errMsg));

		WebResource webResource = buildWebResource(getETLBasePath() + "/jobs/"+idString + "/setError", params);

		ClientResponse response = webResource.get(ClientResponse.class);
		System.out.println(">>> " + response);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Unable to set job error. : "+ response.getStatus());
		}

		ETLJob result = response.getEntity(ETLJob.class);

		return result;
	}

	public ETLJob sendTransformComplete(String idString)
	{
		WebResource webResource = buildWebResource(getETLBasePath() + "/jobs/"+idString + "/transformComplete");

		ClientResponse response = webResource.get(ClientResponse.class);
		System.out.println(">>> " + response);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Unable to send transform complete. : "+ response.getStatus());
		}

		ETLJob result = response.getEntity(ETLJob.class);

		return result;
	}
	
	public void sendExport(ETLFile.Type type, String tableName, String whereClause) {
		
		String typePath;

		switch (type) {
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
			System.err.println("Type "+type+" not supported for queries.");
			System.exit(1);
			return;
		}

		WebResource webResource = buildWebResource(getETLBasePath() + "/query/" + typePath);

		QueryDTO query = new QueryDTO();
		query.setDestination(Destination.ToStaging);
		query.setTableName(tableName);
		query.setQueryString(whereClause);

		ClientResponse response = webResource.type(MediaType.APPLICATION_JSON_TYPE).post(ClientResponse.class, query);
		System.out.println(">>> " + response);

		if (response.getStatus() != 204) {
			System.err.println("Unable to send export : "+ response.getStatus());

			BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntityInputStream()));
			String line;
			try {
				while ((line = reader.readLine()) != null) {
					System.err.println(line);
				}
			} catch (IOException e) {
				System.err.println("An error occurred trying to read the error message from the server.");
			}
		}

	}
}