package be.nabu.eai.server;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlRootElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.eai.repository.Notification;
import be.nabu.libs.authentication.api.Token;
import be.nabu.libs.events.api.EventHandler;
import be.nabu.libs.events.api.EventSubscription;
import be.nabu.libs.http.api.HTTPRequest;
import be.nabu.libs.http.api.HTTPResponse;
import be.nabu.libs.http.api.server.AuthenticationHeader;
import be.nabu.libs.http.api.server.TokenResolver;
import be.nabu.libs.http.core.HTTPUtils;
import be.nabu.libs.http.server.HTTPServerUtils;
import be.nabu.libs.http.server.nio.MemoryMessageDataProvider;
import be.nabu.libs.http.server.nio.NIOHTTPServer;
import be.nabu.libs.http.server.websockets.WebSocketHandshakeHandler;
import be.nabu.libs.http.server.websockets.WebSocketUtils;
import be.nabu.libs.http.server.websockets.api.WebSocketMessage;
import be.nabu.libs.http.server.websockets.api.WebSocketRequest;
import be.nabu.libs.http.server.websockets.impl.WebSocketRequestParserFactory;
import be.nabu.libs.nio.PipelineUtils;
import be.nabu.libs.nio.api.Pipeline;
import be.nabu.libs.nio.api.StandardizedMessagePipeline;
import be.nabu.libs.nio.api.events.ConnectionEvent;
import be.nabu.libs.nio.api.events.ConnectionEvent.ConnectionState;
import be.nabu.utils.mime.api.Header;

public class CollaborationListener {
	
	private Server server;
	private Logger logger = LoggerFactory.getLogger(getClass());

	public CollaborationListener(Server server) {
		this.server = server;
	}
	
	public void start() {
		// listen for websocket requests
		WebSocketHandshakeHandler handshakeHandler = new WebSocketHandshakeHandler(server.getHTTPServer().getDispatcher(), new MemoryMessageDataProvider(), false);
		handshakeHandler.setTokenResolver(new TokenResolver() {
			@Override
			public Token getToken(Header... headers) {
				AuthenticationHeader authenticationHeader = HTTPUtils.getAuthenticationHeader(headers);
				return authenticationHeader == null ? null : authenticationHeader.getToken();
			}
		});
		
		EventSubscription<HTTPRequest, HTTPResponse> subscription = server.getHTTPServer().getDispatcher(null).subscribe(HTTPRequest.class, handshakeHandler);
		subscription.filter(HTTPServerUtils.limitToPath("/collaborate"));
		
		// unlimited lifetime
		((NIOHTTPServer) server.getHTTPServer()).setMaxLifeTime(0l);
		
		server.getHTTPServer().getDispatcher().subscribe(WebSocketRequest.class, new EventHandler<WebSocketRequest, WebSocketMessage>() {
			@Override
			public WebSocketMessage handle(WebSocketRequest event) {
				if ("/collaborate".equals(event.getPath())) {
					CollaborationMessage message = unmarshal(event.getData(), CollaborationMessage.class);
					
					// respond with a pooooiiinnnnggg
					if (CollaborationMessageType.PING.equals(message.getType())) {
						return WebSocketUtils.newMessage(marshal(new CollaborationMessage(CollaborationMessageType.PONG)));
					}
					else if (CollaborationMessageType.HELLO.equals(message.getType())) {
						return newUserList();
					}
					// broadcast to everyone on the collaborate path
					else {
						message.setAlias(event.getToken() == null ? null : event.getToken().getName());
						broadcast(WebSocketUtils.newMessage(marshal(message)), Arrays.asList(PipelineUtils.getPipeline()));
					}
				}
				return null;
			}
		});
		
		server.getHTTPServer().getDispatcher().subscribe(ConnectionEvent.class, new EventHandler<ConnectionEvent, Void>() {
			@SuppressWarnings("unchecked")
			@Override
			public Void handle(ConnectionEvent event) {
				if (ConnectionState.CLOSED.equals(event.getState()) || ConnectionState.UPGRADED.equals(event.getState())) {
					WebSocketRequestParserFactory parserFactory = WebSocketUtils.getParserFactory(event.getPipeline());
					if (parserFactory != null && "/collaborate".equals(parserFactory.getPath())) {
						Token token = WebSocketUtils.getToken((StandardizedMessagePipeline<WebSocketRequest, WebSocketMessage>) event.getPipeline());
						CollaborationMessage content = ConnectionState.UPGRADED.equals(event.getState())
							? new CollaborationMessage(CollaborationMessageType.JOIN, "Connected")
							: new CollaborationMessage(CollaborationMessageType.LEAVE, "Disconnected");
						content.setAlias(token == null ? null : token.getName());
						broadcast(WebSocketUtils.newMessage(marshal(content)), Arrays.asList(event.getPipeline()));
					}
				}
				return null;
			}
		});
		
		server.getRepository().getEventDispatcher().subscribe(Notification.class, new EventHandler<Notification, Void>() {
			@Override
			public Void handle(Notification event) {
				try {
					// note that notification properties are not serialized as they are transient (because they are an object)
					// send the notifications to everyone who connected
					WebSocketMessage newMessage = WebSocketUtils.newMessage(marshal(new CollaborationMessage(CollaborationMessageType.NOTIFICATION, marshal(event))));
					broadcast(newMessage, null);
				}
				catch (Exception e) {
					logger.error("Can not marshal notification", e);
				}
				return null;
			}
		});
	}
	
	public void broadcast(WebSocketMessage message, List<Pipeline> blacklist) {
		List<StandardizedMessagePipeline<WebSocketRequest, WebSocketMessage>> pipelines = WebSocketUtils.getWebsocketPipelines((NIOHTTPServer) server.getHTTPServer(), "/collaborate");
		if (pipelines != null) {
			for (StandardizedMessagePipeline<WebSocketRequest, WebSocketMessage> pipeline : pipelines) {
				if (blacklist == null || !blacklist.contains(pipeline)) {
					pipeline.getResponseQueue().add(message);
				}
			}
		}
	}
	
	public WebSocketMessage newUserList() {
		List<StandardizedMessagePipeline<WebSocketRequest, WebSocketMessage>> pipelines = WebSocketUtils.getWebsocketPipelines((NIOHTTPServer) server.getHTTPServer(), "/collaborate");
		List<User> users = new ArrayList<User>();
		for (StandardizedMessagePipeline<WebSocketRequest, WebSocketMessage> pipeline : pipelines) {
			Token token = WebSocketUtils.getToken(pipeline);
			User user = new User();
			user.setAlias(token == null ? "$anonymous" : token.getName());
			users.add(user);
		}
		Collections.sort(users);
		UserList userList = new UserList();
		userList.setUsers(users);
		return WebSocketUtils.newMessage(marshal(new CollaborationMessage(CollaborationMessageType.USERS, marshal(userList))));
	}
	
	public static <T> byte [] marshal(T content) {
		try {
			Marshaller marshaller = JAXBContext.newInstance(content.getClass()).createMarshaller();
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			marshaller.marshal(content, output);
			return output.toByteArray();
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T unmarshal(InputStream input, Class<T> clazz) {
		try {
			Unmarshaller unmarshaller = JAXBContext.newInstance(clazz).createUnmarshaller();
			return (T) unmarshaller.unmarshal(input);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public enum CollaborationMessageType {
		PING, PONG, HELLO, USERS, UPDATE, DELETE, CREATE, LOCK, UNLOCK, JOIN, LEAVE, LOG, LOCKS, NOTIFICATION
	}
	
	@XmlRootElement
	public static class CollaborationMessage {
		private String id, content, alias;
		private CollaborationMessageType type;
		
		public CollaborationMessage() {
			// auto construct
		}
		public CollaborationMessage(CollaborationMessageType type) {
			this.type = type;
		}
		public CollaborationMessage(CollaborationMessageType type, byte [] content) {
			this.type = type;
			this.content = new String(content, Charset.forName("UTF-8"));
		}
		public CollaborationMessage(CollaborationMessageType type, String content, String id) {
			this.type = type;
			this.content = content;
			this.id = id;
		}
		public CollaborationMessage(CollaborationMessageType type, String content) {
			this.type = type;
			this.content = content;
		}
		
		public String getId() {
			return id;
		}
		public void setId(String id) {
			this.id = id;
		}

		public CollaborationMessageType getType() {
			return type;
		}
		public void setType(CollaborationMessageType type) {
			this.type = type;
		}

		public String getContent() {
			return content;
		}
		public void setContent(String content) {
			this.content = content;
		}
		public String getAlias() {
			return alias;
		}
		public void setAlias(String alias) {
			this.alias = alias;
		}
	}

	public static class User implements Comparable<User> {
		private String alias;

		public String getAlias() {
			return alias;
		}
		public void setAlias(String alias) {
			this.alias = alias;
		}
		@Override
		public int compareTo(User o) {
			return o.alias.compareTo(alias);
		}
	}
	
	@XmlRootElement
	public static class UserList {
		private List<User> users;

		public List<User> getUsers() {
			return users;
		}
		public void setUsers(List<User> users) {
			this.users = users;
		}
	}
}