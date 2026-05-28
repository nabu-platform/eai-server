package be.nabu.eai.server.rest;

public interface MCPToolProvider<I> {

	MCPToolDefinition getToolDefinition(MCPToolDefinitionContext context);

	Class<I> getInputType();

	MCPToolResult invoke(I input, MCPToolCallContext context) throws Exception;
}
