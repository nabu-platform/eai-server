package be.nabu.eai.server.rest;

import java.util.Map;

import be.nabu.libs.authentication.api.Token;
import be.nabu.libs.http.api.HTTPRequest;

public class MCPToolCallContext {

	private final HTTPRequest request;
	private final Token token;
	private final String sessionId;
	private final MCPConfiguration configuration;
	private final Map<String, Object> meta;
	private final boolean preview;

	public MCPToolCallContext(HTTPRequest request, Token token, String sessionId, MCPConfiguration configuration, Map<String, Object> meta, boolean preview) {
		this.request = request;
		this.token = token;
		this.sessionId = sessionId;
		this.configuration = configuration;
		this.meta = meta;
		this.preview = preview;
	}

	public HTTPRequest getRequest() {
		return request;
	}

	public Token getToken() {
		return token;
	}

	public String getSessionId() {
		return sessionId;
	}

	public MCPConfiguration getConfiguration() {
		return configuration;
	}

	public Map<String, Object> getMeta() {
		return meta;
	}

	public boolean isPreview() {
		return preview;
	}
}
