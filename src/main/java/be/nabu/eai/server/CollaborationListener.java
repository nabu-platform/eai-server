package be.nabu.eai.server;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.nio.charset.Charset;
import java.nio.file.FileStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlRootElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.eai.repository.Notification;
import be.nabu.eai.repository.events.ResourceEvent;
import be.nabu.eai.server.ServerStatistics.ServerStatistic;
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
import be.nabu.libs.metrics.impl.SystemMetrics;
import be.nabu.libs.nio.PipelineUtils;
import be.nabu.libs.nio.api.Pipeline;
import be.nabu.libs.nio.api.StandardizedMessagePipeline;
import be.nabu.libs.nio.api.events.ConnectionEvent;
import be.nabu.libs.nio.api.events.ConnectionEvent.ConnectionState;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.binding.api.Window;
import be.nabu.libs.types.binding.xml.XMLBinding;
import be.nabu.utils.mime.api.Header;

public class CollaborationListener {
	
	private Server server;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private boolean broadcastStatistics = true;

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
				logger.debug("Incoming websocket message from source for path: {}", event.getPath());
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
		server.getRepository().getEventDispatcher().subscribe(ResourceEvent.class, new EventHandler<ResourceEvent, Void>() {
			@Override
			public Void handle(ResourceEvent event) {
				CollaborationMessageType type;
				switch(event.getState()) {
					case CREATE: type = CollaborationMessageType.CREATE; break;
					case DELETE: type = CollaborationMessageType.DELETE; break;
					default: type = CollaborationMessageType.UPDATE;
				}
				String id = event.getArtifactId();
				if (event.getPath() != null) {
					id += ":" + event.getPath().replaceFirst("^[/]+", "");
				}
				broadcast(WebSocketUtils.newMessage(marshal(new CollaborationMessage(type, "Direct Server Action", id))), new ArrayList<Pipeline>());
				return null;
			}
		});
		
		final List<FileStore> filestores = SystemMetrics.filestores();
		final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
		final OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
		// push server statistics to the developers
		Thread statisticsThread = new Thread(new Runnable() {
			@Override
			public void run() {
				while (broadcastStatistics) {
					List<User> users = getUsers();
					if (users != null && !users.isEmpty()) {
						ServerStatistics statistics = new ServerStatistics();
						statistics.setUsers(users);
						
						Date date = new Date();
						List<ServerStatistic> metrics = new ArrayList<ServerStatistic>();
						metrics.add(new ServerStatistic("runtime", "load", date.getTime(), ((operatingSystemMXBean.getSystemLoadAverage() / operatingSystemMXBean.getAvailableProcessors()) * 100)));
						
						MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
						metrics.add(new ServerStatistic("memory", "heap", date.getTime(), ((100.0 * heapMemoryUsage.getUsed()) / heapMemoryUsage.getMax())));
						
//						MemoryUsage nonHeapMemoryUsage = memoryMXBean.getNonHeapMemoryUsage();
//						metrics.add(new ServerStatistic("memory", "nonHeap", date.getTime(), ((100.0 * nonHeapMemoryUsage.getUsed()) / nonHeapMemoryUsage.getMax())));
						
						for (FileStore store : filestores) {
							try {
								
								metrics.add(new ServerStatistic("file", store.name(), date.getTime(), (((1.0 * (store.getTotalSpace() - store.getUsableSpace())) / store.getTotalSpace()) * 100)));
							}
							catch (IOException e) {
								logger.warn("Can't read file statistics for " + store.name(), e);
							}
						}
						statistics.setStatistics(metrics);
						
						CollaborationMessage message = new CollaborationMessage(CollaborationMessageType.STATISTICS, marshal(statistics));
						broadcast(WebSocketUtils.newMessage(marshal(message)), Arrays.asList());
						
					}
					try {
						Thread.sleep(10000l);
					}
					catch (InterruptedException e) {
						continue; 
					}
				}
			}
		});
		statisticsThread.setDaemon(true);
		statisticsThread.setName("developer-statistics");
		
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
						
						if (ConnectionState.UPGRADED.equals(event.getState())) {
							// we only start the thread once at least one developer connects
							if (statisticsThread.getState() == Thread.State.NEW) {
								statisticsThread.start();
							}
							// let's send a first status
							else {
								statisticsThread.interrupt();
							}
						}
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
		
		
		// not enabled yet
//		server.getRepository().getComplexEventDispatcher().subscribe(Object.class, new EventHandler<Object, Void>() {
//			@Override
//			public Void handle(Object event) {
//				if (event != null) {
//					EventSeverity severity = null;
//					// if we have an event that has a low severity, skip it
//					if (event instanceof ComplexEvent) {
//						severity = ((ComplexEvent) event).getSeverity();
//					}
//					else if (event instanceof ComplexContent && ((ComplexContent) event).getType().get("severity") != null) {
//						Object object = ((ComplexContent) event).get("severity");
//						if (object instanceof EventSeverity) {
//							severity = (EventSeverity) object;
//						}
//					}
//					if (severity != null && severity.ordinal() < EventSeverity.INFO.ordinal()) {
//						return null;
//					}
//					if (!(event instanceof ComplexContent)) {
//						event = ComplexContentWrapperFactory.getInstance().getWrapper().wrap(event);
//					}
//					if (event != null) {
//						WebSocketMessage newMessage = WebSocketUtils.newMessage(marshal(new CollaborationMessage(CollaborationMessageType.EVENT, marshalComplex((ComplexContent) event))));
//						broadcast(newMessage, null);
//					}
//				}
//				return null;
//			}
//		});
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
		List<User> users = getUsers();
		UserList userList = new UserList();
		userList.setUsers(users);
		return WebSocketUtils.newMessage(marshal(new CollaborationMessage(CollaborationMessageType.USERS, marshal(userList))));
	}

	private List<User> getUsers() {
		List<StandardizedMessagePipeline<WebSocketRequest, WebSocketMessage>> pipelines = WebSocketUtils.getWebsocketPipelines((NIOHTTPServer) server.getHTTPServer(), "/collaborate");
		List<User> users = new ArrayList<User>();
		for (StandardizedMessagePipeline<WebSocketRequest, WebSocketMessage> pipeline : pipelines) {
			Token token = WebSocketUtils.getToken(pipeline);
			User user = new User();
			user.setAlias(token == null ? "$anonymous" : token.getName());
			users.add(user);
		}
		Collections.sort(users);
		return users;
	}
	
	public static byte [] marshalComplex(ComplexContent content) {
		try {
			XMLBinding binding = new XMLBinding(content.getType(), Charset.forName("UTF-8"));
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			binding.marshal(output, content);
			return output.toByteArray();
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public static ComplexContent unmarshalComplex(byte [] bytes, ComplexType type) {
		try {
			XMLBinding binding = new XMLBinding(type, Charset.forName("UTF-8"));
			return binding.unmarshal(new ByteArrayInputStream(bytes), new Window[0]);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
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
		PING, PONG, HELLO, USERS, UPDATE, DELETE, CREATE, LOCK, UNLOCK, JOIN, LEAVE, LOG, LOCKS, NOTIFICATION, REQUEST_LOCK, EVENT, STATISTICS
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
