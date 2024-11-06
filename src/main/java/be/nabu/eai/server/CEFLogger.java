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

package be.nabu.eai.server;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.eai.server.rest.ServerREST;
import be.nabu.libs.events.api.EventHandler;
import be.nabu.utils.cep.api.ComplexEvent;
import be.nabu.utils.cep.api.EventSeverity;
import be.nabu.utils.cep.impl.CEPUtils;
import be.nabu.utils.cep.impl.ComplexEventImpl;

public class CEFLogger implements EventHandler<Object, Void> {

	private Logger logger = LoggerFactory.getLogger("cef");
	private String serverVersion;
	private Server server;
	private String host;
	
	public CEFLogger(Server server) {
		this.server = server;
		this.serverVersion = new ServerREST().getVersion();
		int indexOf = serverVersion.indexOf(':');
		if (indexOf > 0) {
			serverVersion = serverVersion.substring(indexOf + 1).trim();
		}
		try {
			host = InetAddress.getLocalHost().getHostName();
		}
		catch (UnknownHostException e) {
			// ignore
		}
	}
	
	@Override
	public Void handle(Object event) {
		StringBuilder builder = new StringBuilder();
		
		EventSeverity severity = null;
		if (event instanceof ComplexEvent) {
			severity = ((ComplexEvent) event).getSeverity();
		}
		
		if (event instanceof ComplexEventImpl) {
			((ComplexEventImpl) event).setServerGroup(server.getRepository().getGroup());
			((ComplexEventImpl) event).setServerName(server.getRepository().getName());
			((ComplexEventImpl) event).setServerHost(host);
			if (((ComplexEventImpl) event).getTimezone() == null) {
				((ComplexEventImpl) event).setTimezone(TimeZone.getDefault());
			}
			if (((ComplexEventImpl) event).getDuration() == null && ((ComplexEventImpl) event).getStarted() != null && ((ComplexEventImpl) event).getStopped() != null) {
				((ComplexEventImpl) event).setDuration(((ComplexEventImpl) event).getStopped().getTime() - ((ComplexEventImpl) event).getStarted().getTime());
			}
		}
		
		if (severity == null) {
			severity = EventSeverity.INFO;
		}
		
		CEPUtils.asCEF(builder, "Nabu Platform", "Nabu Server", serverVersion, true, Arrays.asList(event));
		
		switch (severity) {
			case TRACE: logger.trace(builder.toString()); break;
			case DEBUG: logger.debug(builder.toString()); break;
			case INFO: logger.info(builder.toString()); break;
			case WARNING: logger.warn(builder.toString()); break;
			case ERROR:
			case CRITICAL: logger.error(builder.toString()); break;
		}
		
		return null;
	}
}
