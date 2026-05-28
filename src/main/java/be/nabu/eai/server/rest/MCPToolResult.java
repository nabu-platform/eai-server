package be.nabu.eai.server.rest;

import java.util.Map;

public class MCPToolResult {

	private final Object structuredContent;
	private final String content;
	private final Map<String, Object> meta;
	private final Boolean isError;
	private final String message;

	public MCPToolResult(Object structuredContent, String content, Map<String, Object> meta) {
		this(structuredContent, content, meta, null, null);
	}

	public MCPToolResult(Object structuredContent, String content, Map<String, Object> meta, Boolean isError, String message) {
		this.structuredContent = structuredContent;
		this.content = content;
		this.meta = meta;
		this.isError = isError;
		this.message = message;
	}

	public Object getStructuredContent() {
		return structuredContent;
	}

	public String getContent() {
		return content;
	}

	public Map<String, Object> getMeta() {
		return meta;
	}

	public Boolean getIsError() {
		return isError;
	}

	public String getMessage() {
		return message;
	}
}
