package org.vena.etltool;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import javax.ws.rs.core.MediaType;

import org.vena.api.customer.authentication.LoginResult;
import org.vena.api.etl.ETLJob;
import org.vena.etltool.entities.CreateModelRequestDTO;
import org.vena.etltool.entities.CreateModelResponseDTO;
import org.vena.id.Id;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;

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

	public ETLClient() {
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
	public CreateModelResponseDTO createModel(String modelName) {

		Client client = Client.create();

		client.addFilter(new HTTPBasicAuthFilter(apiUser, apiKey));

		String uri = protocol+"://"+host+":"+port+"/api/models";
		System.out.println("Calling " + uri);

		WebResource webResource = client.resource(uri);

		webResource.accept("application/json");

		CreateModelRequestDTO createModelDTO = new CreateModelRequestDTO();

		createModelDTO.setName(modelName);
		createModelDTO.setDesc(modelName);

		ClientResponse response = webResource.type(MediaType.APPLICATION_JSON_TYPE).post(ClientResponse.class, createModelDTO);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Login failed : HTTP error code : "+ response.getStatus());
		}

		CreateModelResponseDTO result = response.getEntity(CreateModelResponseDTO.class);

		this.modelId = result.getId();
		
		return result;

	}
	
	public ETLJob requestJobStatus(String idString)
	{
		Client client = Client.create();

		client.addFilter(new HTTPBasicAuthFilter(apiUser, apiKey));

		String uri = protocol+"://"+host+":"+port+"/api/etl/jobs/"+idString;
		System.out.println("Calling " + uri);
		
		WebResource webResource = client.resource(uri);

		webResource.accept("application/json");


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
		Client client = Client.create();

		client.addFilter(new HTTPBasicAuthFilter(apiUser, apiKey));

		String uri = protocol+"://"+host+":"+port+"/api/etl/jobs/"+idString + "/setError" + 
				(errMsg == null ? "" : "?message=" + URLEncoder.encode(errMsg, "UTF-8"));
		System.out.println("Calling " + uri);

		WebResource webResource = client.resource(uri);

		webResource.accept("application/json");


		ClientResponse response = webResource.get(ClientResponse.class);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Unable to set job error. : "+ response.getStatus());
		}

		ETLJob result = response.getEntity(ETLJob.class);

		return result;
	}

	public ETLJob sendTransformComplete(String idString)
	{
		Client client = Client.create();

		client.addFilter(new HTTPBasicAuthFilter(apiUser, apiKey));

		String uri = protocol+"://"+host+":"+port+"/api/models";
		System.out.println("Calling " + uri);
		
		WebResource webResource = client.resource(uri);

		webResource.accept("application/json");


		ClientResponse response = webResource.get(ClientResponse.class);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Unable to send transform complete. : "+ response.getStatus());
		}

		ETLJob result = response.getEntity(ETLJob.class);

		return result;
	}
}