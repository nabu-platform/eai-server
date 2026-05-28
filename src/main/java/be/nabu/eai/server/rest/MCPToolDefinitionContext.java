package be.nabu.eai.server.rest;

public class MCPToolDefinitionContext {

	private final MCPConfiguration configuration;

	public MCPToolDefinitionContext(MCPConfiguration configuration) {
		this.configuration = configuration;
	}

	public MCPConfiguration getConfiguration() {
		return configuration;
	}
}
