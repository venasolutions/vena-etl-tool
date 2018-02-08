package org.vena.etltool;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vena.etltool.ETLClient;
import org.vena.etltool.JerseyClientFactory;
import org.vena.etltool.tests.ETLToolTest.ExitException;
import org.vena.etltool.tests.ETLToolTest.NoExitSecurityManager;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.WebResource.Builder;

public class ETLClientTest {

	protected static final SecurityManager originalManager = System.getSecurityManager();

	ETLClient etlClient;
	JerseyClientFactory clientFactory;
	Client mockClient;
	WebResource mockWebResource;
	Builder mockBuilder;
	ClientResponse mockResponse;

	@BeforeClass
	public static void setNoExitSecurityManager() {
		System.setSecurityManager(new NoExitSecurityManager());
	}

	@AfterClass
	public static void tearDown() {
		System.setSecurityManager(originalManager);
	}

	@Before
	public void beforeTest() {
		clientFactory = mock(JerseyClientFactory.class);
		mockClient = mock(Client.class);
		mockWebResource = mock(WebResource.class);
		mockBuilder = mock(Builder.class);
		mockResponse = mock(ClientResponse.class);

		when(clientFactory.create()).thenReturn(mockClient);
		when(mockClient.resource(anyString())).thenReturn(mockWebResource);
		when(mockWebResource.accept(anyString())).thenReturn(mockBuilder);
		when(mockBuilder.header(anyString(), any())).thenReturn(mockBuilder);
		when(mockBuilder.post(ClientResponse.class)).thenReturn(mockResponse);
		when(mockResponse.getEntityInputStream()).thenReturn(new InputStream() {
			@Override
			public int read() throws IOException {
				// Return EOF
				return -1;
			}
		});

		etlClient = new ETLClient(clientFactory);
		etlClient.username = "user@unit.test";
		etlClient.password = "mockpassword";
	}

	@Test
	public void testLoginSuccess() {
		when(mockResponse.getStatus()).thenReturn(200);
		when(mockResponse.getEntity(String.class)).thenReturn(
				"{\"apiUser\": \"123.456\", \"apiKey\": \"mockkey\", \"location\": \"mock.location\"}");

		try {
			etlClient.login();
		} catch (ExitException ee) {
			fail();
		}
		verify( mockClient ).resource("https://vena.io/login");
		verify( mockBuilder, times(1) ).post(ClientResponse.class);

		assertEquals("mock.location", etlClient.location);
		assertEquals("123.456", etlClient.apiUser);
		assertEquals("mockkey", etlClient.apiKey);
	}

	@Test
	public void testLoginProtocolHost() {
		when(mockResponse.getStatus()).thenReturn(200);
		when(mockResponse.getEntity(String.class)).thenReturn(
				"{\"apiUser\": \"123.456\", \"apiKey\": \"mockkey\", \"location\": \"mock.location\"}");

		try {
			etlClient.protocol = "ftp";
			etlClient.host = "another.io";
			etlClient.login();
		} catch (ExitException ee) {
			fail();
		}
		verify( mockClient ).resource("ftp://another.io/login");
		verify( mockBuilder, times(1) ).post(ClientResponse.class);

		assertEquals("mock.location", etlClient.location);
		assertEquals("123.456", etlClient.apiUser);
		assertEquals("mockkey", etlClient.apiKey);
	}

	@Test
	public void testLoginRetry500() {
		when(mockResponse.getStatus()).thenReturn(500);

		try {
			etlClient.login();
		} catch (ExitException ee) {
			assertEquals(1, ee.status);
			verify( mockClient ).resource("https://vena.io/login");
			// All login hosts should be tried once
			for (String host : ETLClient.LOGIN_HOSTS) {
				verify( mockClient ).resource("https://" + host + "/login");
			}
			verify( mockBuilder, times(ETLClient.LOGIN_HOSTS.size() + 1) ).post(ClientResponse.class);
			return;
		}
		fail();
	}

	@Test
	public void testLoginRetry404() {
		when(mockResponse.getStatus()).thenReturn(404);

		try {
			etlClient.login();
		} catch (ExitException ee) {
			assertEquals(1, ee.status);
			verify( mockClient ).resource("https://vena.io/login");
			// All login hosts should be tried once
			for (String host : ETLClient.LOGIN_HOSTS) {
				verify( mockClient ).resource("https://" + host + "/login");
			}
			verify( mockBuilder, times(ETLClient.LOGIN_HOSTS.size() + 1) ).post(ClientResponse.class);
			return;
		}
		fail();
	}

	@Test
	public void testLoginNoRetry401() {
		when(mockResponse.getStatus()).thenReturn(401);

		try {
			etlClient.login();
		} catch (ExitException ee) {
			assertEquals(1, ee.status);
			verify( mockClient ).resource("https://vena.io/login");
			verify( mockBuilder, times(1) ).post(ClientResponse.class);
			return;
		}
		fail();
	}

	@Test
	public void testLoginNoRetryHost() {
		when(mockResponse.getStatus()).thenReturn(500);

		try {
			// No retry on login hosts should be done if host is explicitly specified
			etlClient.host = "another.io";
			etlClient.login();
		} catch (ExitException ee) {
			assertEquals(1, ee.status);
			verify( mockClient ).resource("https://another.io/login");
			verify( mockBuilder, times(1) ).post(ClientResponse.class);
			return;
		}
		fail();
	}
}
