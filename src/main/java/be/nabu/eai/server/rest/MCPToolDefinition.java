package be.nabu.eai.server.rest;

import java.util.Map;

public class MCPToolDefinition {

	private final String name;
	private final String title;
	private final String description;
	private final boolean previewSupported;
	private final Map<String, Object> annotations;
	private final Map<String, Object> inputSchema;
	private final Map<String, Object> outputSchema;
	private final Map<String, Object> meta;

	public MCPToolDefinition(String name, String title, String description, boolean previewSupported, Map<String, Object> annotations, Map<String, Object> inputSchema, Map<String, Object> outputSchema, Map<String, Object> meta) {
		this.name = name;
		this.title = title;
		this.description = description;
		this.previewSupported = previewSupported;
		this.annotations = annotations;
		this.inputSchema = inputSchema;
		this.outputSchema = outputSchema;
		this.meta = meta;
	}

	public String getName() {
		return name;
	}

	public String getTitle() {
		return title;
	}

	public String getDescription() {
		return description;
	}

	public boolean isPreviewSupported() {
		return previewSupported;
	}

	public Map<String, Object> getAnnotations() {
		return annotations;
	}

	public Map<String, Object> getInputSchema() {
		return inputSchema;
	}

	public Map<String, Object> getOutputSchema() {
		return outputSchema;
	}

	public Map<String, Object> getMeta() {
		return meta;
	}
}
