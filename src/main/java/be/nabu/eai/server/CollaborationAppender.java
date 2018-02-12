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
