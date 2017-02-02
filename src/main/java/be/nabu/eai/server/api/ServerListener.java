package be.nabu.eai.server.api;

import java.util.Comparator;

import be.nabu.eai.server.Server;
import be.nabu.libs.http.api.server.HTTPServer;

public interface ServerListener {
	
	public void listen(Server server, HTTPServer httpServer);
	
	public default Phase getPhase() {
		return Phase.ARTIFACTS_STARTED;
	}
	
	public default Priority getPriority() {
		return Priority.MEDIUM;
	}
	
	public enum Phase {
		REPOSITORY_LOADED,
		ARTIFACTS_STARTED
	}
	
	// the lower the priority, the later you are invoked
	public enum Priority {
		HIGH,
		MEDIUM,
		LOW
	}
	
	public static class ServerListenerComparator implements Comparator<ServerListener> {

		@Override
		public int compare(ServerListener o1, ServerListener o2) {
			return o1.getPriority().ordinal() - o2.getPriority().ordinal();
		}
		
	}
}
