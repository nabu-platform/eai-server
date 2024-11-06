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

import java.util.Arrays;
import java.util.List;

import be.nabu.eai.repository.logger.NabuLogAppender;
import be.nabu.eai.repository.logger.NabuLogMessage;
import be.nabu.eai.server.CollaborationListener.CollaborationMessage;
import be.nabu.eai.server.CollaborationListener.CollaborationMessageType;
import be.nabu.libs.http.server.websockets.WebSocketUtils;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

public class CollaborationAppender extends AppenderBase<ILoggingEvent> {

	private List<String> stackOptionList = Arrays.asList("full");
	private Server server;
	
	public CollaborationAppender(Server server) {
		this.server = server;
	}
	
	@Override
	protected void append(ILoggingEvent event) {
		NabuLogMessage message = NabuLogAppender.toMessage(event, server.getRepository(), stackOptionList);
		server.getCollaborationListener().broadcast(WebSocketUtils.newMessage(CollaborationListener.marshal(new CollaborationMessage(CollaborationMessageType.LOG, CollaborationListener.marshal(message)))), null);
	}

}
