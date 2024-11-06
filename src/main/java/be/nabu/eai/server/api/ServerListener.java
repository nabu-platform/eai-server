/*
* Copyright (C) 2015 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

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
