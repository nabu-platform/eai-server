package be.nabu.eai.server;

import java.io.IOException;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.security.Principal;

import javax.net.ssl.SSLContext;

import be.nabu.libs.http.api.client.HTTPClient;
import be.nabu.libs.http.client.DefaultHTTPClient;
import be.nabu.libs.http.client.SPIAuthenticationHandler;
import be.nabu.libs.http.client.connections.PooledConnectionHandler;
import be.nabu.libs.http.core.CustomCookieStore;

public class ServerConnection {
	
	private HTTPClient client;
	private RemoteServer remote;
	private String host;
	private Integer port;
	private SSLContext context;
	private Principal principal;
	
	public ServerConnection(SSLContext context, Principal principal, String host, Integer port) {
		this.context = context;
		this.principal = principal;
		this.host = host;
		this.port = port;
	}
	
	public URI getMavenRoot() throws IOException {
		try {
			return getRemote().getMavenRoot();
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public String getName() {
		try {
			return getRemote().getName();
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public URI getRepositoryRoot() throws IOException {
		try {
			return getRemote().getRepositoryRoot();
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public HTTPClient getClient() {
		if (client == null) {
			synchronized(this) {
				client = new DefaultHTTPClient(new PooledConnectionHandler(context, 5), new SPIAuthenticationHandler(), new CookieManager(new CustomCookieStore(), CookiePolicy.ACCEPT_ALL), false);
			}
		}
		return client;
	}
	
	public RemoteServer getRemote() {
		if (remote == null) {
			synchronized(this) {
				try {
					remote = new RemoteServer(getClient(), new URI("http://" + host + ":" + port), principal, Charset.forName("UTF-8"));
				}
				catch (URISyntaxException e) {
					throw new RuntimeException(e);
				}
			}
		}
		return remote;
	}
	
	public String getHost() {
		return host;
	}

	public Integer getPort() {
		return port;
	}

	public SSLContext getContext() {
		return context;
	}

	public Principal getPrincipal() {
		return principal;
	}
	
}
