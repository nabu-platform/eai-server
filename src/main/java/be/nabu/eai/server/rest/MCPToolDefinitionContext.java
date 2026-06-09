package be.nabu.eai.server.rest;

import be.nabu.eai.server.Server;

public class MCPToolDefinitionContext {

	private final Server server;
	private final MCPConfiguration configuration;

	public MCPToolDefinitionContext(Server server, MCPConfiguration configuration) {
		this.server = server;
		this.configuration = configuration;
	}

	public Server getServer() {
		return server;
	}

	public MCPConfiguration getConfiguration() {
		return configuration;
	}
}
