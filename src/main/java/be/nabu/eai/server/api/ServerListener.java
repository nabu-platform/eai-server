package be.nabu.eai.server.api;

import be.nabu.eai.server.Server;
import be.nabu.libs.http.api.server.HTTPServer;

public interface ServerListener {
	public void listen(Server server, HTTPServer httpServer);
}
