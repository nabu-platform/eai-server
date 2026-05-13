package be.nabu.eai.server.rest;

import java.io.Serializable;

public class MCPSession implements Serializable {

	private static final long serialVersionUID = 1L;
	private MCPConfiguration configuration;
	private long lastUsed;

	public MCPSession() {
		// default constructor for clustered serialization
	}

	public MCPSession(MCPConfiguration configuration) {
		this.configuration = configuration;
		touch();
	}

	public MCPConfiguration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(MCPConfiguration configuration) {
		this.configuration = configuration;
	}

	public long getLastUsed() {
		return lastUsed;
	}

	public void setLastUsed(long lastUsed) {
		this.lastUsed = lastUsed;
	}

	public void touch() {
		this.lastUsed = System.currentTimeMillis();
	}

	public boolean isExpired() {
		return lastUsed < System.currentTimeMillis() - MCPREST.getSessionTimeout();
	}
}
