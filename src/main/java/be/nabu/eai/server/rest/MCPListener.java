package be.nabu.eai.server.rest;

import be.nabu.eai.server.Server;
import be.nabu.eai.server.api.ServerListener;
import be.nabu.libs.http.api.HTTPRequest;
import be.nabu.libs.http.api.server.HTTPServer;
import be.nabu.libs.http.server.rest.RESTHandler;

public class MCPListener implements ServerListener {

	@Override
	public void listen(Server server, HTTPServer httpServer) {
		if (!server.isEnableMCP()) {
			return;
		}
		httpServer.getDispatcher().subscribe(HTTPRequest.class, new RESTHandler("/", MCPREST.class, null, server));
	}

	@Override
	public Priority getPriority() {
		return Priority.HIGH;
	}
}
