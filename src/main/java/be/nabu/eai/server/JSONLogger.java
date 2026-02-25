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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.eai.server.rest.ServerREST;
import be.nabu.libs.events.api.EventHandler;
import be.nabu.libs.types.ComplexContentWrapperFactory;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.binding.json.JSONBinding;
import be.nabu.libs.types.binding.json.JSONBinding.JSONDynamicBinding;
import be.nabu.utils.cep.api.EventSeverity;
import be.nabu.utils.cep.impl.ComplexEventImpl;

public class JSONLogger implements EventHandler<Object, Void> {

	private Logger logger = LoggerFactory.getLogger("jsonl");
	private String serverVersion;
	private Server server;
	private String host;
	
	public JSONLogger(Server server) {
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
	
	private JSONDynamicLogMapper logMapper = new JSONDynamicLogMapper();
	
	@SuppressWarnings("unchecked")
	@Override
	public Void handle(Object event) {
		
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

		try {
			ComplexContent wrapped = ComplexContentWrapperFactory.getInstance().getWrapper().wrap(event);
			if (wrapped != null) {
				JSONBinding binding = new JSONBinding(wrapped.getType());
				binding.setDynamicBinding(logMapper);
				ByteArrayOutputStream output = new ByteArrayOutputStream();
				binding.marshal(output, wrapped);
				Object object = wrapped.get("severity");
				EventSeverity severity = object instanceof EventSeverity ? (EventSeverity) object : 
					(object == null ? EventSeverity.INFO : EventSeverity.valueOf(object.toString()));
				
				String json = new String(output.toByteArray()); 
				switch (severity) {
					case TRACE: logger.trace(json); break;
					case DEBUG: logger.debug(json); break;
					case INFO: logger.info(json); break;
					case NOTIFICATION:
					case WARNING: logger.warn(json); break;
					default: logger.error(json); break;
				}
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	public static class JSONDynamicLogMapper implements JSONDynamicBinding {
		
		private Map<String, String> mappings = Map.ofEntries(
			Map.entry("artifactId", "service.name"),
			Map.entry("serverGroup", "service.namespace"),
			Map.entry("serverName", "service.instance.id"),
			
			Map.entry("serverHost", "host.name"),
			Map.entry("imageEnvironment", "deployment.environment.name"),
			Map.entry("conversationId", "trace_id"),
			Map.entry("correlationId", "event.context.id"),
			Map.entry("narrativeId", "business.process.id"),
			Map.entry("sessionId", "session.id"),
			Map.entry("alias", "user.name"),
			Map.entry("authenticationId", "user.id"),
			Map.entry("deviceId", "device.id"),
			
			Map.entry("externalId", "event.reference"),
			Map.entry("origin", "event.provider"),
			Map.entry("eventName", "event.name"),
			Map.entry("eventCategory", "event.category"),
			Map.entry("duration", "event.duration"),
			Map.entry("stacktrace", "exception.stacktrace"),
			Map.entry("action", "event.action"),
			Map.entry("code", "event.code"),
			
			Map.entry("started", "event.start_time"),
			Map.entry("stopped", "event.end_time"),
			Map.entry("timezone", "process.timezone"),
			
			Map.entry("sourceIp", "client.address"),
			Map.entry("sourceHost", "client.socket.address"),
			Map.entry("sourcePort", "client.port"),
			
			Map.entry("destinationIp", "server.address"),
			Map.entry("destinationHost", "server.socket.address"),
			Map.entry("destinationPort", "server.port"),
			
			Map.entry("applicationProtocol", "network.protocol.name"),
			Map.entry("networkProtocol", "network.type"),
			Map.entry("transportProtocol", "network.transport"),
			
			Map.entry("sizeIn", "network.io.bytes.receive"),
			Map.entry("sizeOut", "network.io.bytes.transmit"),
			
			Map.entry("imageVersion", "service.version"),
			Map.entry("imageName", "container.image.name"),
			
			Map.entry("sourceLongitude", "client.geo.location.lon"),
			Map.entry("sourceLatitude", "client.geo.location.lat"),
			Map.entry("destinationLongitude", "server.geo.location.lon"),
			Map.entry("destinationLatitude", "server.geo.location.lat"),
			
			Map.entry("impersonatorId", "user.impersonator.id"),
			Map.entry("impersonator", "user.impersonator.name"),
			
			Map.entry("requestUri", "url.full"),
		    Map.entry("method", "http.request.method"),
		    Map.entry("userAgent", "user_agent.original"),
		    Map.entry("language", "browser.language"),
		    Map.entry("bot", "user_agent.bot"),
		    Map.entry("responseCode", "http.response.status_code"),
		    
		    Map.entry("severity", "severity_text"),
		    
		    Map.entry("fileUri", "file.path"),
		    Map.entry("fileSize", "file.size")
		);
		
		@Override
		public String getName(ComplexContent parent, Element<?> element) {
			if (element.getName().equals("message")) {
				// if we have a reason, it should take precedence
				return parent.get("reason") == null 
					? "body"
					: "event.message_template";
			}
			else if (element.getName().equals("reason")) {
				// make sure we dont reuse the message key if there is no actual reason value
				return parent.get("reason") == null
					? "reason"
					: "body";
			}
			return mappings.get(element.getName());
		}
		@Override
		public Element<?> getElement(ComplexType parent, String jsonName) {
			// TODO: not really used currently
			return parent.get(jsonName);
		}
		
	}
}
