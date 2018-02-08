package org.vena.etltool;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.config.ClientConfig;

public class JerseyClientFactory {

	public Client create() {
		return Client.create();
	}

	public Client create(ClientConfig config) {
		return Client.create(config);
	}

}
