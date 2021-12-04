package be.nabu.eai.server;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.nio.charset.Charset;
import java.security.Principal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.HazelcastInstanceNotActiveException;

import be.nabu.eai.authentication.api.PasswordAuthenticator;
import be.nabu.eai.repository.EAIRepositoryUtils;
import be.nabu.eai.repository.api.ClusteredServer;
import be.nabu.eai.repository.api.ExecutorServiceProvider;
import be.nabu.eai.repository.api.MavenRepository;
import be.nabu.eai.repository.api.Node;
import be.nabu.eai.repository.api.Repository;
import be.nabu.eai.repository.api.ResourceRepository;
import be.nabu.eai.repository.events.NodeEvent;
import be.nabu.eai.repository.events.NodeEvent.State;
import be.nabu.eai.repository.events.RepositoryEvent;
import be.nabu.eai.repository.events.RepositoryEvent.RepositoryState;
import be.nabu.eai.repository.logger.NabuLogAppender;
import be.nabu.eai.repository.util.CombinedAuthenticator;
import be.nabu.eai.repository.util.NodeUtils;
import be.nabu.eai.repository.util.SystemPrincipal;
import be.nabu.eai.server.api.ServerListener;
import be.nabu.eai.server.api.ServerListener.Phase;
import be.nabu.eai.server.rest.ServerREST;
import be.nabu.libs.artifacts.api.Artifact;
import be.nabu.libs.artifacts.api.OfflineableArtifact;
import be.nabu.libs.artifacts.api.RestartableArtifact;
import be.nabu.libs.artifacts.api.StartableArtifact;
import be.nabu.libs.artifacts.api.TwoPhaseStartableArtifact;
import be.nabu.libs.artifacts.api.TwoPhaseStoppableArtifact;
import be.nabu.libs.artifacts.api.StartableArtifact.StartPhase;
import be.nabu.libs.artifacts.api.StoppableArtifact;
import be.nabu.libs.artifacts.api.TwoPhaseOfflineableArtifact;
import be.nabu.libs.authentication.api.Authenticator;
import be.nabu.libs.authentication.api.RoleHandler;
import be.nabu.libs.authentication.api.Token;
import be.nabu.libs.authentication.api.principals.BasicPrincipal;
import be.nabu.libs.authentication.impl.BasicPrincipalImpl;
import be.nabu.libs.cluster.api.ClusterBlockingQueue;
import be.nabu.libs.cluster.api.ClusterInstance;
import be.nabu.libs.cluster.api.ClusterMember;
import be.nabu.libs.cluster.api.ClusterMembershipListener;
import be.nabu.libs.cluster.api.ClusterMessageListener;
import be.nabu.libs.cluster.api.ClusterTopic;
import be.nabu.libs.events.api.EventHandler;
import be.nabu.libs.events.api.EventSubscription;
import be.nabu.libs.events.impl.EventDispatcherImpl;
import be.nabu.libs.http.api.HTTPRequest;
import be.nabu.libs.http.api.HTTPResponse;
import be.nabu.libs.http.api.server.HTTPServer;
import be.nabu.libs.http.api.server.RealmHandler;
import be.nabu.libs.http.server.BasicAuthenticationHandler;
import be.nabu.libs.http.server.FixedRealmHandler;
import be.nabu.libs.http.server.HTTPServerUtils;
import be.nabu.libs.http.server.nio.RoutingMessageDataProvider;
import be.nabu.libs.http.server.rest.RESTHandler;
import be.nabu.libs.maven.CreateResourceRepositoryEvent;
import be.nabu.libs.maven.DeleteResourceRepositoryEvent;
import be.nabu.libs.resources.ResourceFactory;
import be.nabu.libs.resources.ResourceUtils;
import be.nabu.libs.resources.api.Resource;
import be.nabu.libs.resources.api.ResourceContainer;
import be.nabu.libs.resources.remote.server.ResourceREST;
import be.nabu.libs.resources.snapshot.SnapshotUtils;
import be.nabu.libs.services.ServiceRunnable;
import be.nabu.libs.services.ServiceRuntime;
import be.nabu.libs.services.api.ClusteredServiceRunner;
import be.nabu.libs.services.api.DefinedService;
import be.nabu.libs.services.api.ExecutionContext;
import be.nabu.libs.services.api.NamedServiceRunner;
import be.nabu.libs.services.api.Service;
import be.nabu.libs.services.api.ServiceException;
import be.nabu.libs.services.api.ServiceResult;
import be.nabu.libs.services.api.ServiceRunnableObserver;
import be.nabu.libs.services.api.ServiceRunner;
import be.nabu.libs.services.pojo.POJOUtils;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.binding.api.Window;
import be.nabu.libs.types.binding.xml.XMLBinding;
import be.nabu.utils.aspects.AspectUtils;
import be.nabu.utils.cep.api.EventSeverity;
import be.nabu.utils.cep.impl.ComplexEventImpl;
import ch.qos.logback.classic.AsyncAppender;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;

public class Server implements NamedServiceRunner, ClusteredServiceRunner, ClusteredServer, ExecutorServiceProvider {
	
	public static final String SERVICE_THREAD_POOL = "be.nabu.eai.server.serviceThreadPoolSize";
	public static final String SERVICE_MAX_CACHE_SIZE = "be.nabu.eai.server.maxCacheSize";
	public static final String SERVICE_MAX_CACHE_ENTRY_SIZE = "be.nabu.eai.server.maxCacheEntrySize";
	
	private Map<String, MemberState> members = new HashMap<String, MemberState>();
	// whether or not the server is in "offline" mode (default not of course)
	private boolean offline;
	private MavenRepository repository;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private List<ServiceRunnable> runningServices = new ArrayList<ServiceRunnable>();
	private boolean anonymousIsRoot;
	private boolean enableSnapshots;
	private int port, listenerPoolSize;
	
	// always have a local instance for local locks etc
	private ClusterInstance cluster;

	private RoleHandler roleHandler;
	
	private List<String> aliases = new ArrayList<String>();
	private boolean disableStartup = false;
	
	/**
	 * This is set to true while the repository is loading
	 * This allows us to queue actions (in the delayedArtifacts) to be done after the repository is done loading
	 * For example if we have an artifact that has to start(), it might depend on another artifact, so we have to finish loading first
	 */
	private boolean isRepositoryLoading = false;
	private List<NodeEvent> delayedNodeEvents = new ArrayList<NodeEvent>();
	
	private Date startupTime, reloadTime;
	private boolean enabledRepositorySharing, forceRemoteRepository;
	private boolean isStarted;
	private PasswordAuthenticator passwordAuthenticator;
	private ResourceContainer<?> deployments;
	private Appender<ILoggingEvent> appender;
	private List<ServerListener> serverListeners;
	private HTTPServer httpServer;
	private Thread queueExecutionThread;
	private ExecutorService pool;
	private CollaborationListener collaborationListener;
	private boolean shuttingDown;
	private MultipleCEPProcessor processor;
	private ComplexEventImpl startupEvent;
	private Map<String, BatchResultFuture> futures = new HashMap<String, BatchResultFuture>();
	private Runnable startedListener;
	private boolean selfMonitor;
	
	Server(RoleHandler roleHandler, MavenRepository repository, ComplexEventImpl startupEvent) throws IOException {
		this.roleHandler = roleHandler;
		this.repository = repository;
		this.startupEvent = startupEvent;
	}
	
	private void initializeListeners() {
		ClusterInstance cluster = getCluster();
		if (cluster != null) {
			logger.info("Initializing cluster listeners");
			// for $any
			final ClusterBlockingQueue<ServiceExecutionTask> queue = cluster.queue("server.execute");
			queueExecutionThread = new Thread(new Runnable() {
				public void run() {
					while (true) {
						try {
							ServiceExecutionTask task = queue.take();
							Server.this.run(task);
						}
						catch (Exception e) {
							logger.error("Could not execute task", e);
						}
					}
				}
			});
			queueExecutionThread.start();
			
			// for $all
			ClusterTopic<ServiceExecutionTask> topic = cluster.topic("server.execute");
			topic.subscribe(new ClusterMessageListener<ServiceExecutionTask>() {
				@Override
				public void onMessage(ServiceExecutionTask message) {
					try {
						run(message);
					}
					catch (Exception e) {
						logger.error("Could not execute task", e);
					}
				}
			});
			
			// feeding back the result
			ClusterTopic<ServiceExecutionResult> resultTopic = cluster.topic("server.result");
			resultTopic.subscribe(new ClusterMessageListener<ServiceExecutionResult>() {
				@Override
				public void onMessage(ServiceExecutionResult message) {
					try {
						feedback(message);
					}
					catch (Exception e) {
						logger.error("Could not report result", e);
					}
				}
			});
			
			for (ClusterMember member : cluster.members()) {
				registerMember(member);
			}
			
			cluster.addMembershipListener(new ClusterMembershipListener() {
				@Override
				public void memberRemoved(ClusterMember member) {
					ComplexEventImpl memberEvent = new ComplexEventImpl();
					memberEvent.setCode("MEMBER-LEFT");
					memberEvent.setEventName("cluster-member-left");
					memberEvent.setCreated(new Date());
					// if this server is offline, we are assuming a maintenance window or something like it, it only trigger a warning
					// in any other situation, we trigger an error, unless someone is pushing on the buttons, this is bad!
					memberEvent.setSeverity(isOffline() ? EventSeverity.WARNING : EventSeverity.ERROR);
					memberEvent.setTimezone(TimeZone.getDefault());
					memberEvent.setMessage("Member left cluster: " + member.getName() + " (group: " + member.getGroup() + ")");
					repository.getComplexEventDispatcher().fire(memberEvent, Server.this);
					removeMember(member);
				}
				@Override
				public void memberAdded(ClusterMember member) {
					ComplexEventImpl memberEvent = new ComplexEventImpl();
					memberEvent.setCode("MEMBER-JOINED");
					memberEvent.setEventName("cluster-member-joined");
					memberEvent.setCreated(new Date());
					memberEvent.setSeverity(EventSeverity.INFO);
					memberEvent.setTimezone(TimeZone.getDefault());
					memberEvent.setMessage("Member joined cluster: " + member.getName() + " (group: " + member.getGroup() + ")");
					repository.getComplexEventDispatcher().fire(memberEvent, Server.this);
					registerMember(member);
				}
			});
			
			ClusterTopic<HeartbeatMessage> heartbeatTopic = cluster.topic("server.heartbeat");
			heartbeatTopic.subscribe(new ClusterMessageListener<HeartbeatMessage>() {
				@Override
				public void onMessage(HeartbeatMessage message) {
					try {
						processHeartbeat(message);
					}
					catch (Exception e) {
						logger.error("Could not process heartbeat", e);
					}
				}
			});
		}
	}
	
	private void processHeartbeat(HeartbeatMessage message) {
		// TODO
	}
	
	private void registerMember(ClusterMember member) {
		synchronized(members) {
			MemberState memberState = new MemberState();
			memberState.setName(member.getName());
			memberState.setGroup(member.getGroup());
			members.put(member.getName() + "@" + member.getGroup(), memberState);
		}
	}
	private void removeMember(ClusterMember member) {
		synchronized(members) {
			members.remove(member.getName() + "@" + member.getGroup());
		}
	}
	
	private ServiceResult toResult(ServiceExecutionResult execution) {
		ComplexContent output = null;
		ServiceException exception = null;
		
		if (execution.getErrorLog() != null) {
			exception = new ServiceException(execution.getErrorCode() == null ? "REMOTE-1" : execution.getErrorCode(), execution.getErrorLog());
		}
		if (execution.getOutput() != null) {
			Artifact service = repository.resolve(execution.getServiceId());
			if (service instanceof DefinedService) {
				XMLBinding binding = new XMLBinding(((DefinedService) service).getServiceInterface().getOutputDefinition(), Charset.forName("UTF-8"));
				try {
					output = binding.unmarshal(new ByteArrayInputStream(execution.getOutput().getBytes(Charset.forName("UTF-8"))), new Window[0]);
				}
				catch (Exception e) {
					exception = new ServiceException("REMOTE-2", "Could not parse output received from remote server", e);
				}
			}
		}
		
		ServiceException finalException = exception;
		ComplexContent finalOutput = output;
		return new ServiceResult() {
			@Override
			public ComplexContent getOutput() {
				return finalOutput;
			}
			@Override
			public ServiceException getException() {
				return finalException;
			}
		};
	}
	
	private void feedback(ServiceExecutionResult result) {
		if (futures.containsKey(result.getRunId())) {
			futures.get(result.getRunId()).addResult(toResult(result));
		}
	}

	private Future<ServiceResult> run(ServiceExecutionTask task) throws IOException, ParseException {
		DefinedService service = (DefinedService) repository.resolve(task.getServiceId());
		if (service == null) {
			throw new IllegalArgumentException("Could not find service: " + task.getServiceId());
		}
		ServiceRunner target = null;
		// if we have a target, we likely mean for example an execution pool
		if (task.getTarget() != null) {
			ServiceRunner intendedTarget = (ServiceRunner) repository.resolve(task.getTarget());
			// if we found the target, use that
			if (intendedTarget != null) {
				target = intendedTarget;
			}
			// otherwise, check if we mean this server or this server group
			else if (task.getTarget().equals(getName()) || task.getTarget().equals(repository.getGroup()) || (repository.getAliases() != null && repository.getAliases().contains(task.getTarget()))) {
				target = Server.this;
			}
		}
		else {
			target = Server.this;
		}
		// if we don't have a target, ignore this
		if (target != null) {
			ComplexContent input = null;
			if (task.getInput() != null) {
				XMLBinding binding = new XMLBinding(service.getServiceInterface().getInputDefinition(), Charset.forName("UTF-8"));
				input = binding.unmarshal(new ByteArrayInputStream(task.getInput().getBytes(Charset.forName("UTF-8"))), new Window[0]);
			}
			runInPool(service, repository.newExecutionContext(SystemPrincipal.ROOT), input, target, new ResultHandler() {
				@Override
				public void handle(ServiceResult result) {
					ClusterInstance cluster = getCluster();
					// we should only report our results if we had a run id to tie it to and we are living in a clustered world
					if (cluster != null && task.getRunId() != null) {
						ServiceExecutionResult output = new ServiceExecutionResult();
						output.setRunId(task.getRunId());
						output.setTarget(getName());
						output.setServiceId(task.getServiceId());
						if (result.getOutput() != null) {
							XMLBinding binding = new XMLBinding(service.getServiceInterface().getInputDefinition(), Charset.forName("UTF-8"));
							ByteArrayOutputStream baos = new ByteArrayOutputStream();
							try {
								binding.marshal(baos, result.getOutput());
							}
							catch (IOException e) {
								throw new RuntimeException(e);
							}
							output.setOutput(new String(baos.toByteArray(), Charset.forName("UTF-8")));
						}
						if (result.getException() != null) {
							output.setErrorCode(result.getException().getCode());
							StringWriter writer = new StringWriter();
							PrintWriter printer = new PrintWriter(writer);
							result.getException().printStackTrace(printer);
							printer.flush();
							output.setErrorLog(writer.toString());
						}
						cluster.topic("server.result").publish(output);
					}
				}
			});
		}
		return null;
	}
	
	public void runInPool(final Service service, final ExecutionContext context, final ComplexContent content) {
		runInPool(service, context, content, null, null);
	}
	
	public static interface ResultHandler {
		public void handle(ServiceResult result);
	}
	
	public void runInPool(final Service service, final ExecutionContext context, final ComplexContent content, ServiceRunner runner, ResultHandler handler) {
		pool.submit(new Runnable() {
			public void run() {
				try {
					ServiceRunner target = runner == null ? Server.this : runner;
					Future<ServiceResult> run = target.run(service, context, content);
					ServiceResult serviceResult = run.get();
					if (serviceResult != null && serviceResult.getException() != null) {
						logger.error("Could not run service" + (service instanceof DefinedService ? ": " + ((DefinedService) service).getId() : ""), serviceResult.getException());	
					}
					if (handler != null) {
						handler.handle(serviceResult);
					}
				}
				catch (Exception e) {
					logger.error("Failed to run service" + (service instanceof DefinedService ? ": " + ((DefinedService) service).getId() : ""), e);
				}
			}
		});
	}
	
	public ResourceContainer<?> getDeployments() {
		return deployments;
	}
	public void setDeployments(ResourceContainer<?> deployments) {
		this.deployments = deployments;
	}

	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	
	public boolean enableLogger(String loggerService, boolean async, Map<String, String> properties) {
		if (loggerService != null) {
			Artifact resolve = repository.resolve(loggerService);
			if (resolve == null) {
				logger.error("Can not find logger service, disabling logger");
				return false;
			}
			
			LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
			NabuLogAppender nabuAppender = new NabuLogAppender(getRepository(), (DefinedService) resolve, properties);
			nabuAppender.setContext(loggerContext);
			if (async) {
				appender = new AsyncAppender();
				appender.setContext(loggerContext);
				((AsyncAppender) appender).addAppender(nabuAppender);
				nabuAppender.start();
			}
			else {
				appender = nabuAppender;
			}
			appender.setName("Nabu Custom Logger");
			Logger logger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
//			Logger logger = LoggerFactory.getLogger("be.nabu");
			// don't set it to error, this should be configured, the service can still drop all the non-error logs
//			((ch.qos.logback.classic.Logger) logger).setLevel(ch.qos.logback.classic.Level.ERROR);
//			((ch.qos.logback.classic.Logger) logger).setAdditive(true);
			((ch.qos.logback.classic.Logger) logger).addAppender(appender);
			appender.start();
			logger.info("Registered central appender '" + loggerService + "' to " + Logger.ROOT_LOGGER_NAME);
			return true;
		}
		return false;
	}

	public boolean enableSecurity(String authenticationService) {
		if (authenticationService != null) {
			Artifact resolve = repository.resolve(authenticationService);
			if (resolve == null) {
				logger.error("Can not find authentication service, disabling rest");
				return false;
			}
			passwordAuthenticator = POJOUtils.newProxy(PasswordAuthenticator.class, (DefinedService) resolve, getRepository(), SystemPrincipal.ROOT);
			BasicAuthenticationHandler basicAuthenticationHandler = new BasicAuthenticationHandler(new CombinedAuthenticator(passwordAuthenticator, null), new RealmHandler() {
				@Override
				public String getRealm(HTTPRequest request) {
					return getRepository().getGroup();
				}
			});
			basicAuthenticationHandler.setRequired(true);
			getHTTPServer().getDispatcher(null).subscribe(HTTPRequest.class, basicAuthenticationHandler);
		}
		else {
			BasicAuthenticationHandler basicAuthenticationHandler = new BasicAuthenticationHandler(new Authenticator() {
				@Override
				public Token authenticate(String realm, Principal...credentials) {
					return credentials.length > 0 && credentials[0] instanceof BasicPrincipal
						? new BasicPrincipalImpl(credentials[0].getName(), ((BasicPrincipal) credentials[0]).getPassword(), realm)
						: null;
				}
			}, new FixedRealmHandler(getRepository().getGroup()));
			basicAuthenticationHandler.setRequired(false);
			getHTTPServer().getDispatcher(null).subscribe(HTTPRequest.class, basicAuthenticationHandler);
		}
		return true;
	}

	public void enableREST() {
		// make sure we intercept invoke commands
		getHTTPServer().getDispatcher(null).subscribe(HTTPRequest.class, new RESTHandler("/", ServerREST.class, roleHandler, repository, this));
		
		collaborationListener = new CollaborationListener(this);
		collaborationListener.start();
		
		// set the server logger
		LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
		CollaborationAppender appender = new CollaborationAppender(this);
		appender.setContext(loggerContext);
		AsyncAppender asyncAppender = new AsyncAppender();
		asyncAppender.setContext(loggerContext);
		((AsyncAppender) asyncAppender).addAppender(appender);
		appender.start();
		asyncAppender.setName("Nabu Collaboration Logger");
		Logger logger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		((ch.qos.logback.classic.Logger) logger).addAppender(asyncAppender);
		asyncAppender.start();
		logger.info("Registered collaboration appender to " + Logger.ROOT_LOGGER_NAME);
	}
	
	public CollaborationListener getCollaborationListener() {
		return collaborationListener;
	}

	/**
	 * The node inside the event _can_ be outdated in some reload events, this method gets the latest version of the node
	 * It may need to be cleaned up at some point but in some cases (notably unload) you actually need access to whatever node the event was triggered upon (to shut it down properly) instead of the "latest" version
	 */
	private Node getNode(NodeEvent nodeEvent) {
		Node node = repository.getNode(nodeEvent.getId());
		if (node == null) {
			logger.warn("Could not resolve node for: " + nodeEvent.getId());
			node = nodeEvent.getNode();
		}
		return node;
	}
	
	public void initialize() {
		// make sure we respond to repository events
		repository.getEventDispatcher().subscribe(NodeEvent.class, new EventHandler<NodeEvent, Void>() {
			@Override
			public Void handle(NodeEvent nodeEvent) {
				if (nodeEvent.isDone()) {
					logger.debug("[DONE] Receiving " + nodeEvent.getState() + " for " + nodeEvent.getId());
					// a new node is loaded, let's check if we have to set something up
					if (nodeEvent.getState() == NodeEvent.State.LOAD) {
						if (StartableArtifact.class.isAssignableFrom(nodeEvent.getNode().getArtifactClass())) {
							if (isRepositoryLoading) {
								delayedNodeEvents.add(nodeEvent);
							}
							else {
								start(nodeEvent, true, true);
							}
						}
					}
					else if (nodeEvent.getState() == NodeEvent.State.RELOAD) {
						try {
							if (RestartableArtifact.class.isAssignableFrom(nodeEvent.getNode().getArtifactClass())) {
								if (isRepositoryLoading) {
									delayedNodeEvents.add(nodeEvent);
								}
								else {
									restart((RestartableArtifact) nodeEvent.getNode().getArtifact(), true);
								}
							}
							else if (StartableArtifact.class.isAssignableFrom(nodeEvent.getNode().getArtifactClass())) {
								if (isRepositoryLoading) {
									delayedNodeEvents.add(nodeEvent);
								}
								else {
									start((StartableArtifact) nodeEvent.getNode().getArtifact(), true, true);
								}
							}
						}
						catch (Exception e) {
							logger.error("Failed to load artifact: " + nodeEvent.getId(), e);
						}
					}
					
					// if it's a load or a reload and its a service with an eager setting, execute it
					// the service must not have any inputs!
					if (nodeEvent.getState() == NodeEvent.State.LOAD || nodeEvent.getState() == NodeEvent.State.RELOAD) {
						// only applicable to services
						if (DefinedService.class.isAssignableFrom(nodeEvent.getNode().getArtifactClass())) {
							// if it's eager, execute it
							if (NodeUtils.isEager(nodeEvent.getNode())) {
								if (isRepositoryLoading) {
									delayedNodeEvents.add(nodeEvent);
								}
								else {
									run(nodeEvent);
								}
							}
						}
					}
				}
				else {
					// if a node is unloaded we might need to stop something
					if (nodeEvent.getState() == NodeEvent.State.UNLOAD || nodeEvent.getState() == NodeEvent.State.RELOAD) {
						try {
							logger.debug("[ONGOING] Receiving " + nodeEvent.getState() + " for " + nodeEvent.getId());
							// only proceed if the node is loaded
							if (nodeEvent.getNode().isLoaded()) {
								if (StoppableArtifact.class.isAssignableFrom(nodeEvent.getNode().getArtifactClass())) {
									stop((StoppableArtifact) nodeEvent.getNode().getArtifact(), true);
								}
							}
						}
						catch (Exception e) {
							logger.error("Failed to load artifact: " + nodeEvent.getId(), e);
						}
					}
				}
				return null;
			}
		});
		
		repository.getEventDispatcher().subscribe(RepositoryEvent.class, new EventHandler<RepositoryEvent, Void>() {

			@Override
			public Void handle(RepositoryEvent event) {
				if (event.getState() == RepositoryState.LOAD || event.getState() == RepositoryState.RELOAD) {
					// if the loading is done, toggle the boolean and finish delayed actions
					if (event.isDone()) {
						if (isStarted) {
							logger.info("Repository reloaded in " + ((new Date().getTime() - reloadTime.getTime()) / 1000) + "s, processing artifacts");
						}
						else {
							// TODO: find any modules that want to hook into the server after load (not reload?) so we can for instance register a global security handler etc
							// TODO: want to do this _before_ starting everything up? once the http servers are up, they can start firing requests, if this happens before for example security handlers are set...set are screwed
							logger.info("Repository loaded in " + ((new Date().getTime() - startupTime.getTime()) / 1000) + "s, processing artifacts");
							
							// get all the server listeners
							serverListeners = new ArrayList<ServerListener>();
							
							for (Class<ServerListener> clazz : EAIRepositoryUtils.getImplementationsFor(getRepository().getClassLoader(), ServerListener.class)) {
								try {
									serverListeners.add(clazz.newInstance());
								}
								catch (Exception e) {
									logger.error("Could not initialize server listener: " + clazz, e);
								}
							}
							
							Collections.sort(serverListeners, new ServerListener.ServerListenerComparator());
							
							// all the artifacts are loaded but not yet started
							for (ServerListener serverListener : serverListeners) {
								if (serverListener.getPhase() == Phase.REPOSITORY_LOADED) {
									serverListener.listen(Server.this, getHTTPServer());
								}
							}
						}
						isRepositoryLoading = false;
						// it is possible to get multiple events for one item (e.g. if an unload of a new item triggers an initial load only to be reloaded afterwards)
						// TODO: currently we don't check for unload as the combination of unload+load/reload does not occur just yet
						List<String> items = new ArrayList<String>();
						Iterator<NodeEvent> iterator = delayedNodeEvents.iterator();
						while(iterator.hasNext()) {
							NodeEvent next = iterator.next();
							if (items.contains(next.getId())) {
								iterator.remove();
							}
							else {
								items.add(next.getId());
							}
						}
						orderNodes(repository, delayedNodeEvents);
						// TODO: load in dependency order!
						List<TwoPhaseStartableArtifact> twoPhasers = new ArrayList<TwoPhaseStartableArtifact>();
						for (NodeEvent delayedNodeEvent : delayedNodeEvents) {
							try {
								if (DefinedService.class.isAssignableFrom(delayedNodeEvent.getNode().getArtifactClass()) && NodeUtils.isEager(delayedNodeEvent.getNode())) {
									run(delayedNodeEvent);
								}
								else if (delayedNodeEvent.getState() == State.RELOAD && RestartableArtifact.class.isAssignableFrom(delayedNodeEvent.getNode().getArtifactClass()) && !disableStartup) {
									restart(delayedNodeEvent, false);
								}
								else if (StartableArtifact.class.isAssignableFrom(delayedNodeEvent.getNode().getArtifactClass()) && !disableStartup) {
									// don't recurse, on start we should be starting all the nodes
									start(delayedNodeEvent, false, false);
								}
								if (TwoPhaseStartableArtifact.class.isAssignableFrom(delayedNodeEvent.getNode().getArtifactClass())) {
									twoPhasers.add((TwoPhaseStartableArtifact) getNode(delayedNodeEvent).getArtifact());
								}
							}
							catch (Throwable e) {
								logger.error("Could not run delayed node event: " + delayedNodeEvent.getState() + " on " + delayedNodeEvent.getId(), e);
							}
						}
						boolean offline = isOffline();
						// for two phase artifacts, we wait until everything is done before doing the final phase
						// e.g. a http server should only go live once all the applications on it are loaded
						for (TwoPhaseStartableArtifact twoPhaser : twoPhasers) {
							try {
								if (offline && twoPhaser instanceof TwoPhaseOfflineableArtifact) {
									logger.info("Finalizing offline " + twoPhaser.getClass().getSimpleName() + ": " + twoPhaser.getId());
									((TwoPhaseOfflineableArtifact) twoPhaser).offlineFinish();
								}
								else {
									logger.info("Finalizing online " + twoPhaser.getClass().getSimpleName() + ": " + twoPhaser.getId());
									twoPhaser.finish();
								}
							}
							catch (Throwable e) {
								logger.error("Could not run second phase of artifact: " + twoPhaser.getId(), e);
							}
						}
						if (isStarted) {
							logger.info("Server artifacts reloaded in: " + ((new Date().getTime() - reloadTime.getTime()) / 1000) + "s");
						}
						else {
							logger.info("Server started in " + ((new Date().getTime() - startupTime.getTime()) / 1000) + "s");
							isStarted = true;
							initializeListeners();
							// it may have built up a number of startup exceptions, interesting to send those along
							if (processor != null) {
								if (startupEvent != null) {
									startupEvent.setStopped(new Date());
									startupEvent.setDuration(startupEvent.getStopped().getTime() - startupEvent.getStarted().getTime());
									startupEvent.setTimezone(TimeZone.getDefault());
									repository.getComplexEventDispatcher().fire(startupEvent, Server.this);
								}
								processor.start();
							}
						}
						// clear the delayed node events to free up some memory
						delayedNodeEvents.clear();
					}
					else {
						isRepositoryLoading = true;
						delayedNodeEvents.clear();
						reloadTime = new Date();
					}
				}
				return null;
			}
		});
	}

	private void run(NodeEvent nodeEvent) {
		try {
			ComplexType inputDefinition = ((DefinedService) nodeEvent.getNode().getArtifact()).getServiceInterface().getInputDefinition();
			ServiceRuntime runtime = new ServiceRuntime(
				((DefinedService) getNode(nodeEvent).getArtifact()),
				repository.newExecutionContext(SystemPrincipal.ROOT)
			);
			runtime.setAllowCaching(false);
			runtime.run(inputDefinition.newInstance());
		}
		catch (IOException e) {
			logger.error("Could not load eager service: " + nodeEvent.getId(), e);
		}
		catch (ParseException e) {
			logger.error("Could not load eager service: " + nodeEvent.getId(), e);
		}
		catch (ServiceException e) {
			logger.error("Could not run eager service: " + nodeEvent.getId(), e);
		}
	}
	
	private void restart(NodeEvent nodeEvent, boolean recursive) {
		try {
			restart((RestartableArtifact) getNode(nodeEvent).getArtifact(), recursive);
		}
		catch (IOException e) {
			logger.error("Failed to load artifact: " + nodeEvent.getId(), e);
		}
		catch (ParseException e) {
			logger.error("Failed to load artifact: " + nodeEvent.getId(), e);
		}
	}
	
	private void start(NodeEvent nodeEvent, boolean recursive, boolean finish) {
		try {
			Artifact artifact = getNode(nodeEvent).getArtifact();
			start((StartableArtifact) artifact, recursive, finish);
		}
		catch (IOException e) {
			logger.error("Failed to load artifact: " + nodeEvent.getId(), e);
		}
		catch (ParseException e) {
			logger.error("Failed to load artifact: " + nodeEvent.getId(), e);
		}
	}
	
	public void enableRepository() throws IOException {
		ResourceContainer<?> repositoryRoot = ((ResourceRepository) repository).getRoot().getContainer();
		if (AspectUtils.hasAspects(repositoryRoot)) {
			List<Object> aspects = AspectUtils.aspects(repositoryRoot);
			repositoryRoot = (ResourceContainer<?>) aspects.get(0);
		}
		getHTTPServer().getDispatcher().subscribe(HTTPRequest.class, new RESTHandler("/repository", ResourceREST.class, null, repositoryRoot));
		if (repository.getMavenRoot() != null) {
			getHTTPServer().getDispatcher().subscribe(HTTPRequest.class, new RESTHandler("/modules", ResourceREST.class, null, (ResourceContainer<?>) ResourceFactory.getInstance().resolve(repository.getMavenRoot(), null)));
		}
		this.enabledRepositorySharing = true;
	}
	
	public void enableAlias(String alias, URI uri) {
		try {
			logger.info("Exposing alias '" + alias + "': " + uri);
			ResourceContainer<?> root = ResourceUtils.mkdir(uri, null);
			if (root == null) {
				logger.error("Can not enable alias: " + alias);
			}
			else {
				EventSubscription<HTTPRequest, HTTPResponse> subscription = getHTTPServer().getDispatcher().subscribe(HTTPRequest.class, new RESTHandler("/alias/" + alias, ResourceREST.class, null, root));
				subscription.filter(HTTPServerUtils.limitToPath("/alias/" + alias));
				aliases.add(alias);
			}
		}
		catch (IOException e) {
			logger.error("Can not enable alias: " + alias, e);
		}
	}
	
	public List<String> getAliases() {
		return aliases;
	}

	public void snapshotRepository(String path) throws IOException {
		if (enableSnapshots) {
			ResourceContainer<?> repositoryRoot = ((ResourceRepository) repository).getRoot().getContainer();
			Resource resolve = ResourceUtils.resolve(repositoryRoot, path);
			if (resolve != null) {
				logger.info("Snapshotting: " + ResourceUtils.getURI(resolve) + " / " + resolve.hashCode());
				// snapshot the parent non-recursively because you will likely delete the child resource and we don't want that change affecting lookups until you persist it
				if (resolve.getParent() != null) {
					SnapshotUtils.snapshot(resolve.getParent(), false);
				}
				SnapshotUtils.snapshot(resolve, true);
			}
		}
	}
	public void releaseRepository(String path) throws IOException {
		if (enableSnapshots) {
			ResourceContainer<?> repositoryRoot = ((ResourceRepository) repository).getRoot().getContainer();
			Resource resolve = ResourceUtils.resolve(repositoryRoot, path);
			if (resolve != null) {
				logger.info("Releasing: " + ResourceUtils.getURI(resolve));
				if (resolve.getParent() != null) {
					SnapshotUtils.release(resolve.getParent(), false);
				}
				SnapshotUtils.release(resolve, true);
			}
		}
	}
	public void restoreRepository(String path) throws IOException {
		if (enableSnapshots) {
			ResourceContainer<?> repositoryRoot = ((ResourceRepository) repository).getRoot().getContainer();
			Resource resolve = ResourceUtils.resolve(repositoryRoot, path);
			if (resolve != null && SnapshotUtils.isSnapshotted(resolve)) {
				logger.info("Restoring: " + ResourceUtils.getURI(resolve));
				SnapshotUtils.restore(resolve, true);
			}
			// release it to bring it back in sync
			releaseRepository(path);
		}
		else {
			throw new IllegalStateException("Snapshotting is not enabled on this server, a restore is impossible");
		}
	}
	
	public void enableMaven() {
		repository.getEventDispatcher().subscribe(DeleteResourceRepositoryEvent.class, new EventHandler<DeleteResourceRepositoryEvent, Void>() {
			@Override
			public Void handle(DeleteResourceRepositoryEvent event) {
				logger.info("Deleting maven artifact " + event.getArtifact().getArtifactId());
				repository.unloadMavenArtifact(event.getArtifact());
				return null;
			}
		}).filter(new EventHandler<DeleteResourceRepositoryEvent, Boolean>() {
			@Override
			public Boolean handle(DeleteResourceRepositoryEvent event) {
				return !event.isInternal();
			}
		});
		repository.getEventDispatcher().subscribe(CreateResourceRepositoryEvent.class, new EventHandler<CreateResourceRepositoryEvent, Void>() {
			@Override
			public Void handle(CreateResourceRepositoryEvent event) {
				logger.info("Installing maven artifact " + event.getArtifact().getArtifactId());
				repository.loadMavenArtifact(event.getArtifact());
				return null;
			}
		}).filter(new EventHandler<CreateResourceRepositoryEvent, Boolean>() {
			@Override
			public Boolean handle(CreateResourceRepositoryEvent event) {
				return !event.isInternal();
			}
		});
		// no support for non-root calls atm!
//		getHTTPServer().getDispatcher(null).subscribe(HTTPRequest.class, new MavenListener(repository.getMavenRepository(), "maven"));
	}
	
	private void start(StartableArtifact artifact, boolean recursive, boolean finish) {
		boolean offline = isOffline();
		try {
			if (shuttingDown) {
				throw new RuntimeException("Can't start artifact: " + artifact.getId() + " during shutdown");
			}
			// if we are offline, start in that modus
			if (offline && artifact instanceof OfflineableArtifact) {
				logger.info("Starting offline " + artifact.getClass().getSimpleName() + ": " + artifact.getId());
				((OfflineableArtifact) artifact).startOffline();
			}
			else {
				logger.info("Starting online " + artifact.getClass().getSimpleName() + ": " + artifact.getId());
				artifact.start();
			}
			if (recursive) {
				for (String dependency : repository.getDependencies(artifact.getId())) {
					try {
						if (repository.getNode(dependency).getArtifact() instanceof StartableArtifact) {
							start((StartableArtifact) repository.getNode(dependency).getArtifact(), recursive, finish);
						}
					}
					catch (Exception e) {
						logger.error("Could not start dependency: " + dependency, e);
					}
				}
			}
			if (finish && artifact instanceof TwoPhaseStartableArtifact) {
				// if we are offline, start in that modus
				if (offline && artifact instanceof TwoPhaseOfflineableArtifact) {
					logger.info("Finalizing offline " + artifact.getClass().getSimpleName() + ": " + artifact.getId());
					((TwoPhaseOfflineableArtifact) artifact).offlineFinish();
				}
				else {
					logger.info("Finalizing online " + artifact.getClass().getSimpleName() + ": " + artifact.getId());
					((TwoPhaseStartableArtifact) artifact).finish();
				}
			}
			EAIRepositoryUtils.message(repository, artifact.getId(), "start", true);
		}
		catch (Exception e) {
			logger.error("Error while starting " + artifact.getClass().getSimpleName() + ": " + artifact.getId(), e);
			EAIRepositoryUtils.message(repository, artifact.getId(), "start", true, EAIRepositoryUtils.toValidation(e));
		}
	}
	
	public void restart(String id) {
		Artifact resolved = getRepository().resolve(id);
		// if it is offline, we don't do a "regular" startup, but rather an offline one
		if (resolved instanceof RestartableArtifact && (!offline || !(resolved instanceof OfflineableArtifact))) {
			restart((RestartableArtifact) resolved, true);
		}
		else if (resolved instanceof StartableArtifact && resolved instanceof StoppableArtifact) {
			stop((StoppableArtifact) resolved, true);
			start((StartableArtifact) resolved, true, true);
		}
	}
	
	public void stop(String id) {
		Artifact resolved = getRepository().resolve(id);
		if (resolved instanceof StoppableArtifact) {
			stop((StoppableArtifact) resolved, true);
		}
	}
	
	public void start(String id) {
		Artifact resolved = getRepository().resolve(id);
		if (resolved instanceof StartableArtifact) {
			start((StartableArtifact) resolved, true, true);
		}
	}
	
	private void restart(RestartableArtifact artifact, boolean recursive) {
		logger.info("Restarting " + artifact.getClass().getSimpleName() + ": " + artifact.getId());
		try {
			if (!(artifact instanceof StartableArtifact) || ((StartableArtifact) artifact).isStarted()) {
				artifact.restart();
			}
			else {
				artifact.start();
			}
			if (recursive) {
				for (String dependency : repository.getDependencies(artifact.getId())) {
					try {
						if (repository.getNode(dependency).getArtifact() instanceof RestartableArtifact) {
							if (!(repository.getNode(dependency).getArtifact() instanceof StartableArtifact) || ((StartableArtifact) repository.getNode(dependency).getArtifact()).isStarted()) {
								restart((RestartableArtifact) repository.getNode(dependency).getArtifact(), recursive);
							}
							else {
								start((RestartableArtifact) repository.getNode(dependency).getArtifact(), recursive, true);
							}
						}
					}
					catch (Exception e) {
						logger.error("Could not restart dependency: " + dependency, e);
					}
				}
			}
			EAIRepositoryUtils.message(repository, artifact.getId(), "start", true);
		}
		catch (Exception e) {
			logger.error("Error while restarting " + artifact.getClass().getSimpleName() + ": " + artifact.getId(), e);
			EAIRepositoryUtils.message(repository, artifact.getId(), "start", true, EAIRepositoryUtils.toValidation(e));
		}
	}
	
	private void stop(StoppableArtifact artifact, boolean recursive) {
		try {
			if (!(artifact instanceof StartableArtifact) || ((StartableArtifact) artifact).isStarted()) {
				// first halt
				if (artifact instanceof TwoPhaseStoppableArtifact) {
					logger.info("Halting " + artifact.getClass().getSimpleName() + ": " + artifact.getId());
					((TwoPhaseStoppableArtifact) artifact).halt();
				}
				// then stop
				logger.info("Stopping " + artifact.getClass().getSimpleName() + ": " + artifact.getId());
				artifact.stop();
			}
			if (recursive) {
				for (String dependency : repository.getDependencies(artifact.getId())) {
					try {
						if (repository.getNode(dependency) != null && repository.getNode(dependency).isLoaded() && repository.getNode(dependency).getArtifact() instanceof StoppableArtifact) {
							stop((StoppableArtifact) repository.getNode(dependency).getArtifact(), recursive);
						}
					}
					catch (Exception e) {
						logger.error("Could not stop dependency: " + dependency, e);
					}
				}
			}
			EAIRepositoryUtils.message(repository, artifact.getId(), "stop", true);
		}
		catch (Exception e) {
			logger.error("Error while stopping " + artifact.getClass().getSimpleName() + ": " + artifact.getId(), e);
			EAIRepositoryUtils.message(repository, artifact.getId(), "stop", true, EAIRepositoryUtils.toValidation(e));
		}
	}

	public MavenRepository getRepository() {
		return repository;
	}

	@Override
	public Future<ServiceResult> run(Service service, ExecutionContext executionContext, ComplexContent input, ServiceRunnableObserver...observers) {
		List<ServiceRunnableObserver> allObservers = new ArrayList<ServiceRunnableObserver>(observers.length + 1);
		allObservers.add(new RunningServiceObserver());
		allObservers.addAll(Arrays.asList(observers));
		ServiceRuntime serviceRuntime = new ServiceRuntime(service, executionContext);
		final ServiceRunnable runnable = new ServiceRunnable(serviceRuntime, input, allObservers.toArray(new ServiceRunnableObserver[allObservers.size()]));
		// in the future could actually run this async in a thread pool but for now it is assumed that all originating systems have their own thread pool
		// for example the messaging system runs in its own thread pool, as does the http server etc
		// if we were going for a centralized thread pool those systems should use the central one as well but then might start to interfere with one another
		return new ServiceResultFuture(runnable.call());
	}
	
	public static class ServiceResultFuture implements Future<ServiceResult> {
		
		private ServiceResult serviceResult;
		
		public ServiceResultFuture(ServiceResult serviceResult) {
			this.serviceResult = serviceResult;
		}
		
		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			return false;
		}
		@Override
		public boolean isCancelled() {
			return false;
		}
		@Override
		public boolean isDone() {
			return true;
		}
		@Override
		public ServiceResult get() throws InterruptedException, ExecutionException {
			return serviceResult;
		}
		@Override
		public ServiceResult get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
			return serviceResult;
		}
	}

	private class RunningServiceObserver implements ServiceRunnableObserver {
		@Override
		public void stop(ServiceRunnable serviceRunnable) {
			synchronized(runningServices) {
				runningServices.remove(serviceRunnable);
			}
		}
		@Override
		public void start(ServiceRunnable serviceRunnable) {
			synchronized(runningServices) {
				runningServices.add(serviceRunnable);
			}
		}
	}
	
	public void bringOffline() {
		logger.info("Bringing server offline");
		// check if we starting in offline modus
		String property = System.getProperty("user.home");
		File target = property == null ? new File(".") : new File(property);
		if (!target.exists()) {
			target.mkdirs();
		}
		File offlineFile = new File(target, "nabu.offline");
		if (!offlineFile.exists()) {
			try {
				offlineFile.createNewFile();
			}
			catch (IOException e) {
				logger.warn("Could not create offline marker file, the server will remain online");
				throw new RuntimeException(e);
			}
		}
		offline = true;
		List<TwoPhaseOfflineableArtifact> twoPhasers = new ArrayList<TwoPhaseOfflineableArtifact>();
		List<OfflineableArtifact> artifacts = repository.getArtifacts(OfflineableArtifact.class);
		// reverse sort for shutdown
		artifacts.sort(new Comparator<OfflineableArtifact>() {
			@Override
			public int compare(OfflineableArtifact o1, OfflineableArtifact o2) {
				StartPhase phase1 = o1.getPhase();
				StartPhase phase2 = o2.getPhase();
				return phase2.ordinal() - phase1.ordinal();
			}
		});
		for (OfflineableArtifact artifact : artifacts) {
			try {
				logger.info("Taking " + artifact.getId() + " offline");
				artifact.offline();
				if (artifact instanceof TwoPhaseOfflineableArtifact) {
					twoPhasers.add((TwoPhaseOfflineableArtifact) artifact);
				}
			}
			catch (Exception e) {
				logger.warn("Could not bring artifact " + artifact.getId() + " offline");
			}
		}
		for (TwoPhaseOfflineableArtifact twoPhaser : twoPhasers) {
			try {
				logger.info("Finalizing offline sequence for: " + twoPhaser.getId());
				twoPhaser.offlineFinish();
			}
			catch (Throwable e) {
				logger.error("Could not run second phase of artifact: " + twoPhaser.getId(), e);
			}
		}
	}
	
	public void bringOnline() {
		logger.info("Bringing server online");
		// check if we starting in offline modus
		String property = System.getProperty("user.home");
		File target = property == null ? new File(".") : new File(property);
		if (target.exists()) {
			File offlineFile = new File(target, "nabu.offline");
			if (offlineFile.exists()) {
				if (!offlineFile.delete()) {
					logger.warn("Could not clear offline marker file, the server will remain offline");
					throw new RuntimeException("Could not clear offline marker file, the server will remain offline");
				}
			}
		}
		offline = false;
		List<TwoPhaseOfflineableArtifact> twoPhasers = new ArrayList<TwoPhaseOfflineableArtifact>();
		List<OfflineableArtifact> artifacts = repository.getArtifacts(OfflineableArtifact.class);
		artifacts.sort(new Comparator<OfflineableArtifact>() {
			@Override
			public int compare(OfflineableArtifact o1, OfflineableArtifact o2) {
				StartPhase phase1 = o1.getPhase();
				StartPhase phase2 = o2.getPhase();
				return phase1.ordinal() - phase2.ordinal();
			}
		});
		for (OfflineableArtifact artifact : artifacts) {
			try {
				logger.info("Bringing " + artifact.getId() + " online");
				artifact.online();
				if (artifact instanceof TwoPhaseOfflineableArtifact) {
					twoPhasers.add((TwoPhaseOfflineableArtifact) artifact);
				}
			}
			catch (Exception e) {
				logger.warn("Could not bring artifact " + artifact.getId() + " offline");
			}
		}
		for (TwoPhaseOfflineableArtifact twoPhaser : twoPhasers) {
			try {
				logger.info("Finalizing online sequence for: " + twoPhaser.getId());
				twoPhaser.onlineFinish();
			}
			catch (Throwable e) {
				logger.error("Could not run second phase of artifact: " + twoPhaser.getId(), e);
			}
		}
	}
	
	private boolean calculateOffline() {
		// check if we starting in offline modus
		String property = System.getProperty("user.home");
		File target = property == null ? new File(".") : new File(property);
		if (target.exists() && new File(target, "nabu.offline").exists()) {
			return true;
		}
		else {
			return false;
		}
	}
	
	public boolean isOffline() {
		return offline;
	}
	
	public void start() throws IOException {
		this.offline = calculateOffline();
		
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				logger.info("Shutting down server");
				shuttingDown = true;
				List<StoppableArtifact> artifacts = repository.getArtifacts(StoppableArtifact.class);
				// we want to reverse sort them, if they had to start late, they have to be shut down first
				artifacts.sort(new Comparator<StoppableArtifact>() {
					@Override
					public int compare(StoppableArtifact o1, StoppableArtifact o2) {
						StartPhase phase1 = o1 instanceof StartableArtifact ? ((StartableArtifact) o1).getPhase() : StartPhase.NORMAL;
						StartPhase phase2 = o2 instanceof StartableArtifact ? ((StartableArtifact) o2).getPhase() : StartPhase.NORMAL;
						return phase2.ordinal() - phase1.ordinal();
					}
				});
				// first do an initial pass of all the artifacts that can can be halted before being stopped
				for (StoppableArtifact artifact : artifacts) {
					if (!(artifact instanceof StartableArtifact) || ((StartableArtifact) artifact).isStarted()) {
						if (artifact instanceof TwoPhaseStoppableArtifact) {
							logger.info("Halting " + artifact.getId());
							try {
								((TwoPhaseStoppableArtifact) artifact).halt();
							}
							catch (Exception e) {
								logger.error("Failed to halt " + artifact.getId(), e);
							}
						}
					}
				}
				for (StoppableArtifact artifact : artifacts) {
					// if it is either not startable or running, stop it
					if (!(artifact instanceof StartableArtifact) || ((StartableArtifact) artifact).isStarted()) {
						logger.info("Stopping " + artifact.getId());
						try {
							artifact.stop();
						}
						catch (Exception e) {
							logger.error("Failed to shut down " + artifact.getId(), e);
						}
					}
				}
			}
		}));
		
		startupTime = new Date();
		Thread.currentThread().setContextClassLoader(repository.getClassLoader());
		repository.start();
		
		// everything should be up and running
		for (ServerListener serverListener : serverListeners) {
			if (serverListener.getPhase() == Phase.ARTIFACTS_STARTED) {
				serverListener.listen(Server.this, getHTTPServer());
			}
		}
		
		if (startedListener != null) {
			startedListener.run();
		}
	}
	
	public URI getRepositoryRoot() {
		return ResourceUtils.getURI(((ResourceRepository) repository).getRoot().getContainer());
	}
	
	/**
	 * TODO: Only checks direct references atm, not recursive ones
	 */
	@SuppressWarnings("unused")
	private void orderNodes(final Repository repository, List<NodeEvent> events) {
		Comparator<NodeEvent> comparator = new Comparator<NodeEvent>() {
			@Override
			public int compare(NodeEvent o1, NodeEvent o2) {
//				List<String> references = repository.getReferences(o1.getId());
//				if (references != null && references.contains(o2.getId())) {
//					return 1;
//				}
//				references = repository.getReferences(o2.getId());
//				if (references != null && references.contains(o1.getId())) {
//					return -1;
//				}
//				return 0;
				System.out.println("Checking: " + o1.getId() + " vs " + o2.getId());
				if (isInReferences(repository, o1.getId(), o2.getId(), new ArrayList<String>())) {
					System.out.println("\t1");
					return 1;
				}
				else if (isInReferences(repository, o2.getId(), o1.getId(), new ArrayList<String>())) {
					System.out.println("\t-1");
					return -1;
				}
				System.out.println("\t0");
				return 0;
			}
		};
		Map<String, Set<String>> allReferences = new HashMap<String, Set<String>>();
		for (int i = 0; i < events.size(); i++) {
			allReferences.put(events.get(i).getId(), EAIRepositoryUtils.getAllReferences(repository, events.get(i).getId()));
		}
		Set<String> warnings = new HashSet<String>();
		boolean changed = true;
		sorting: while(changed) {
			changed = false;
			for (int i = 0; i < events.size(); i++) {
				for (int j = 0; j < events.size(); j++) {
					if (i == j) {
						continue;
					}
					// if i is before j but requires it in references, try to switch 
					boolean iDependsOnJ = allReferences.get(events.get(i).getId()).contains(events.get(j).getId());
					boolean jDependsOnI = allReferences.get(events.get(j).getId()).contains(events.get(i).getId());
					
					StartPhase iPhase = StartPhase.NORMAL, jPhase = StartPhase.NORMAL;
					// check for stages for startup
					if (!iDependsOnJ && !jDependsOnI) {
						Artifact iResolve = repository.resolve(events.get(i).getId());
						Artifact jResolve = repository.resolve(events.get(j).getId());
						
						if (iResolve instanceof StartableArtifact && jResolve instanceof StartableArtifact) {
							iPhase = ((StartableArtifact) iResolve).getPhase();
							jPhase = ((StartableArtifact) jResolve).getPhase();
						}
					}
					
					if (jDependsOnI && iDependsOnJ) {
						// to have each warning only once, not once in each direction
						int comparison = events.get(i).getId().compareTo(events.get(j).getId());
						warnings.add("Found circular reference between: " + events.get(comparison < 0 ? i : j) + " and " + events.get(comparison < 0 ? j : i));
					}
					else if ((iDependsOnJ && i < j) || (jDependsOnI && j < i) || (i < j && iPhase.ordinal() > jPhase.ordinal()) || (i > j && jPhase.ordinal() > iPhase.ordinal())) {
						NodeEvent nodeEvent = events.get(i);
						events.set(i, events.get(j));
						events.set(j, nodeEvent);
						changed = true;
						continue sorting;
					}
				}
			}
		}
		for (String warning : warnings) {
			logger.warn(warning);
		}
//		Collections.sort(events, comparator);
	}
	
	private static boolean isInReferences(Repository repository, String startingNode, String searchNode, List<String> searchedNodes) {
		searchedNodes.add(startingNode);
		List<String> references = repository.getReferences(startingNode);
		if (references.contains(searchNode)) {
			return true;
		}
		else {
			for (String reference : references) {
				if (!searchedNodes.contains(reference)) {
					boolean inReferences = isInReferences(repository, reference, searchNode, searchedNodes);
					if (inReferences) {
						return true;
					}
				}
			}
		}
		return false;
	}

	public boolean isEnabledRepositorySharing() {
		return enabledRepositorySharing;
	}

	public boolean isForceRemoteRepository() {
		return forceRemoteRepository;
	}

	public void setForceRemoteRepository(boolean forceRemoteRepository) {
		this.forceRemoteRepository = forceRemoteRepository;
	}

	public boolean isAnonymousIsRoot() {
		return anonymousIsRoot;
	}

	public void setAnonymousIsRoot(boolean anonymousIsRoot) {
		this.anonymousIsRoot = anonymousIsRoot;
	}

	@Override
	public String getName() {
		return repository.getName();
	}

	public PasswordAuthenticator getPasswordAuthenticator() {
		return passwordAuthenticator;
	}

	public boolean isEnableSnapshots() {
		return enableSnapshots;
	}

	public void setEnableSnapshots(boolean enableSnapshots) {
		this.enableSnapshots = enableSnapshots;
	}

	public int getListenerPoolSize() {
		return listenerPoolSize;
	}

	public void setListenerPoolSize(int listenerPoolSize) {
		this.listenerPoolSize = listenerPoolSize;
	}

	public HTTPServer getHTTPServer() {
		if (httpServer == null) {
			synchronized(this) {
				if (httpServer == null) {
					httpServer = HTTPServerUtils.newServer(port, listenerPoolSize, new EventDispatcherImpl());
					httpServer.setMessageDataProvider(new RoutingMessageDataProvider());
					if (Boolean.parseBoolean(System.getProperty("server.encode", "true"))) {
						httpServer.getDispatcher().subscribe(HTTPResponse.class, HTTPServerUtils.ensureContentEncoding());
					}
				}
			}
		}
		return httpServer;
	}
	
	boolean hasHTTPServer() {
		return httpServer != null;
	}

	@Override
	public ClusterInstance getCluster() {
		return cluster;
	}
	public void setCluster(ClusterInstance cluster) {
		this.cluster = cluster;
	}

	@Override
	public void runAnywhere(Service service, ExecutionContext context, ComplexContent input, String target) {
		ServiceExecutionTask task = toTask(service, input, target);
		try {
			cluster.queue("server.execute").put(task);
		}
		catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	private ServiceExecutionTask toTask(Service service, ComplexContent input, String target) {
		ServiceExecutionTask task = new ServiceExecutionTask();
		task.setServiceId(((DefinedService) service).getId());
		if (input != null) {
			XMLBinding binding = new XMLBinding(service.getServiceInterface().getInputDefinition(), Charset.forName("UTF-8"));
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			try {
				binding.marshal(output, input);
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
			task.setInput(new String(output.toByteArray(), Charset.forName("UTF-8")));
		}
		task.setTarget(target);
		return task;
	}

	@Override
	public void runEverywhere(Service service, ExecutionContext context, ComplexContent input, String target) {
		ServiceExecutionTask task = toTask(service, input, target);
		cluster.topic("server.execute").publish(task);
	}

	public ExecutorService getPool() {
		return pool;
	}
	public void setPool(ExecutorService pool) {
		this.pool = pool;
	}

	public boolean isDisableStartup() {
		return disableStartup;
	}

	public void setDisableStartup(boolean disableStartup) {
		this.disableStartup = disableStartup;
	}

	public Date getStartupTime() {
		return startupTime;
	}

	@Override
	public ExecutorService getExecutorService() {
		return pool;
	}

	public MultipleCEPProcessor getProcessor() {
		return processor;
	}

	public void setProcessor(MultipleCEPProcessor processor) {
		this.processor = processor;
	}

	public Runnable getStartedListener() {
		return startedListener;
	}

	public void setStartedListener(Runnable startedListener) {
		this.startedListener = startedListener;
	}
}