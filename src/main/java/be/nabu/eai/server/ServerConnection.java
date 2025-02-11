/*
* Copyright (C) 2015 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

package be.nabu.eai.server;

import java.io.IOException;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.security.Principal;
import java.util.concurrent.Executors;

import javax.net.ssl.SSLContext;

import be.nabu.libs.events.impl.EventDispatcherImpl;
import be.nabu.libs.http.api.client.HTTPClient;
import be.nabu.libs.http.client.DefaultHTTPClient;
import be.nabu.libs.http.client.SPIAuthenticationHandler;
import be.nabu.libs.http.client.connections.PlainConnectionHandler;
import be.nabu.libs.http.client.nio.NIOHTTPClientImpl;
import be.nabu.libs.http.core.CustomCookieStore;
import be.nabu.libs.http.server.nio.MemoryMessageDataProvider;

public class ServerConnection {
	
	private HTTPClient client;
	private RemoteServer remote;
	private String host;
	private Integer port;
	private boolean secure;
	private SSLContext context;
	private Principal principal;
	private String name, path;
	private int socketTimeout = 60*1000*15, connectionTimeout = 60*1000;
	
	public ServerConnection(SSLContext context, Principal principal, String host, Integer port) {
		this(context, principal, host, port, false);
	}
	public ServerConnection(SSLContext context, Principal principal, String host, Integer port, boolean secure) {
		this(context, principal, host, port, secure, "");
	}
	public ServerConnection(SSLContext context, Principal principal, String host, Integer port, boolean secure, String path) {
		this.context = context;
		this.principal = principal;
		this.host = host;
		this.port = port;
		this.secure = secure;
		this.path = path;
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
		if (name == null) {
			name = getRemote().getName();
		}
		return name;
	}
	
	public String getVersion() {
		return getRemote().getVersion();
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
				if (Boolean.parseBoolean(System.getProperty("http.experimental.client", "true"))) {
					client = new NIOHTTPClientImpl(context, 10, 10, 50, new EventDispatcherImpl(), new MemoryMessageDataProvider(), new CookieManager(new CustomCookieStore(), CookiePolicy.ACCEPT_ALL), Executors.defaultThreadFactory());
					// set it to default 5 minutes, the client default is 2 and we hit it too often with longer running services and deployments
					((NIOHTTPClientImpl) client).setRequestTimeout(1000l * 60 * Long.parseLong(System.getProperty("http.timeout", "5")));
				}
				else {
					client = new DefaultHTTPClient(new PlainConnectionHandler(context, connectionTimeout, socketTimeout), new SPIAuthenticationHandler(), new CookieManager(new CustomCookieStore(), CookiePolicy.ACCEPT_ALL), false);
				}
			}
		}
		return client;
	}
	
	public RemoteServer getRemote() {
		if (remote == null) {
			synchronized(this) {
				try {
					remote = new RemoteServer(getClient(), new URI((secure ? "https" : "http") + "://" + host + ":" + port + path), principal, Charset.forName("UTF-8"));
				}
				catch (URISyntaxException e) {
					throw new RuntimeException(e);
				}
			}
		}
		return remote;
	}
	
	public void setRemote(RemoteServer remote) {
		this.remote = remote;
	}
	public String getHost() {
		return host;
	}

	public Integer getPort() {
		return port;
	}
	
	public String getPath() {
		return path;
	}

	public SSLContext getContext() {
		return context;
	}

	public Principal getPrincipal() {
		return principal;
	}

	public int getSocketTimeout() {
		return socketTimeout;
	}

	public void setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
	}

	public int getConnectionTimeout() {
		return connectionTimeout;
	}

	public void setConnectionTimeout(int connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}

	public boolean isSecure() {
		return secure;
	}

	public void setSecure(boolean secure) {
		this.secure = secure;
	}

	public void setPrincipal(Principal principal) {
		this.principal = principal;
	}
	
}
