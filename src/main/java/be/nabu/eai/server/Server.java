package be.nabu.eai.server;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.eai.repository.api.MavenRepository;
import be.nabu.eai.repository.api.Node;
import be.nabu.eai.repository.api.Repository;
import be.nabu.eai.repository.api.ResourceRepository;
import be.nabu.eai.repository.events.NodeEvent;
import be.nabu.eai.repository.events.RepositoryEvent;
import be.nabu.eai.repository.events.NodeEvent.State;
import be.nabu.eai.repository.events.RepositoryEvent.RepositoryState;
import be.nabu.eai.repository.util.NodeUtils;
import be.nabu.eai.repository.util.SystemPrincipal;
import be.nabu.eai.server.api.ServerListener;
import be.nabu.eai.server.rest.ServerREST;
import be.nabu.libs.artifacts.api.Artifact;
import be.nabu.libs.artifacts.api.RestartableArtifact;
import be.nabu.libs.artifacts.api.StartableArtifact;
import be.nabu.libs.artifacts.api.StoppableArtifact;
import be.nabu.libs.authentication.api.RoleHandler;
import be.nabu.libs.events.api.EventHandler;
import be.nabu.libs.http.api.HTTPRequest;
import be.nabu.libs.http.api.server.HTTPServer;
import be.nabu.libs.http.server.rest.RESTHandler;
import be.nabu.libs.maven.CreateResourceRepositoryEvent;
import be.nabu.libs.maven.DeleteResourceRepositoryEvent;
import be.nabu.libs.maven.MavenListener;
import be.nabu.libs.resources.ResourceFactory;
import be.nabu.libs.resources.ResourceUtils;
import be.nabu.libs.resources.remote.server.ResourceREST;
import be.nabu.libs.services.ServiceRunnable;
import be.nabu.libs.services.ServiceRuntime;
import be.nabu.libs.services.SimpleServiceResult;
import be.nabu.libs.services.api.DefinedService;
import be.nabu.libs.services.api.ExecutionContext;
import be.nabu.libs.services.api.Service;
import be.nabu.libs.services.api.ServiceException;
import be.nabu.libs.services.api.ServiceResult;
import be.nabu.libs.services.api.ServiceRunnableObserver;
import be.nabu.libs.services.api.ServiceRunner;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;

public class Server implements ServiceRunner {
	
	public static final String SERVICE_THREAD_POOL = "be.nabu.eai.server.serviceThreadPoolSize";
	public static final String SERVICE_MAX_CACHE_SIZE = "be.nabu.eai.server.maxCacheSize";
	public static final String SERVICE_MAX_CACHE_ENTRY_SIZE = "be.nabu.eai.server.maxCacheEntrySize";
	
	private MavenRepository repository;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private List<ServiceRunnable> runningServices = new ArrayList<ServiceRunnable>();

	private RoleHandler roleHandler;
	
	/**
	 * This is set to true while the repository is loading
	 * This allows us to queue actions (in the delayedArtifacts) to be done after the repository is done loading
	 * For example if we have an artifact that has to start(), it might depend on another artifact, so we have to finish loading first
	 */
	private boolean isRepositoryLoading = false;
	private List<NodeEvent> delayedNodeEvents = new ArrayList<NodeEvent>();
	
	private Date startupTime;
	private boolean enabledRepositorySharing, forceRemoteRepository;
	private boolean isStarted;
	
	public Server(RoleHandler roleHandler, MavenRepository repository) throws IOException {
		this.roleHandler = roleHandler;
		this.repository = repository;
		initialize();
	}

	public void enableREST(HTTPServer server) {
		// make sure we intercept invoke commands
		server.getDispatcher(null).subscribe(HTTPRequest.class, new RESTHandler("/", ServerREST.class, roleHandler, repository, this));
		for (Class<ServerListener> serverListener : getRepository().getImplementationsFor(ServerListener.class)) {
			try {
				serverListener.newInstance().listen(this, server);
			}
			catch (Exception e) {
				logger.error("Could not initialize server listener: " + serverListener, e);
			}
		}
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
					// a new node is loaded, let's check if we have to set something up
					if (nodeEvent.getState() == NodeEvent.State.LOAD) {
						if (StartableArtifact.class.isAssignableFrom(nodeEvent.getNode().getArtifactClass())) {
							if (isRepositoryLoading) {
								delayedNodeEvents.add(nodeEvent);
							}
							else {
								start(nodeEvent, true);
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
									start((StartableArtifact) nodeEvent.getNode().getArtifact(), true);
								}
							}
						}
						catch (IOException e) {
							logger.error("Failed to load artifact: " + nodeEvent.getId(), e);
						}
						catch (ParseException e) {
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
							// only proceed if the node is loaded
							if (nodeEvent.getNode().isLoaded()) {
								if (StoppableArtifact.class.isAssignableFrom(nodeEvent.getNode().getArtifactClass())) {
									stop((StoppableArtifact) nodeEvent.getNode().getArtifact(), true);
								}
							}
						}
						catch (IOException e) {
							logger.error("Failed to load artifact: " + nodeEvent.getId(), e);
						}
						catch (ParseException e) {
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
							logger.info("Repository reloaded in " + ((new Date().getTime() - startupTime.getTime()) / 1000) + "s, processing artifacts");
						}
						else {
							logger.info("Repository loaded in " + ((new Date().getTime() - startupTime.getTime()) / 1000) + "s, processing artifacts");
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
						for (NodeEvent delayedNodeEvent : delayedNodeEvents) {
							try {
								if (DefinedService.class.isAssignableFrom(delayedNodeEvent.getNode().getArtifactClass()) && NodeUtils.isEager(delayedNodeEvent.getNode())) {
									run(delayedNodeEvent);
								}
								else if (delayedNodeEvent.getState() == State.RELOAD && RestartableArtifact.class.isAssignableFrom(delayedNodeEvent.getNode().getArtifactClass())) {
									restart(delayedNodeEvent, false);
								}
								else if (StartableArtifact.class.isAssignableFrom(delayedNodeEvent.getNode().getArtifactClass())) {
									// don't recurse, on start we should be starting all the nodes
									start(delayedNodeEvent, false);
								}
							}
							catch (Throwable e) {
								logger.error("Could not run delayed node event: " + delayedNodeEvent.getState() + " on " + delayedNodeEvent.getId(), e);
							}
						}
						if (isStarted) {
							logger.info("Server artifacts reloaded in: " + ((new Date().getTime() - startupTime.getTime()) / 1000) + "s");
						}
						else {
							logger.info("Server started in " + ((new Date().getTime() - startupTime.getTime()) / 1000) + "s");
							isStarted = true;
						}
					}
					else {
						isRepositoryLoading = true;
						delayedNodeEvents.clear();
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
	
	private void start(NodeEvent nodeEvent, boolean recursive) {
		try {
			start((StartableArtifact) getNode(nodeEvent).getArtifact(), recursive);
		}
		catch (IOException e) {
			logger.error("Failed to load artifact: " + nodeEvent.getId(), e);
		}
		catch (ParseException e) {
			logger.error("Failed to load artifact: " + nodeEvent.getId(), e);
		}
	}
	
	public void enableRepository(HTTPServer server) throws IOException {
		server.getDispatcher().subscribe(HTTPRequest.class, new RESTHandler("/repository", ResourceREST.class, null, ((ResourceRepository) repository).getRoot().getContainer()));
		server.getDispatcher().subscribe(HTTPRequest.class, new RESTHandler("/maven", ResourceREST.class, null, ResourceFactory.getInstance().resolve(repository.getMavenRoot(), null)));
		this.enabledRepositorySharing = true;
	}
	
	public void enableMaven(HTTPServer server) {
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
		server.getDispatcher(null).subscribe(HTTPRequest.class, new MavenListener(repository.getMavenRepository(), "maven"));
	}
	
	private void start(StartableArtifact artifact, boolean recursive) {
		logger.info("Starting " + artifact.getClass().getSimpleName() + ": " + artifact.getId());
		try {
			artifact.start();
			if (recursive) {
				for (String dependency : repository.getDependencies(artifact.getId())) {
					try {
						if (repository.getNode(dependency).getArtifact() instanceof StartableArtifact) {
							start((StartableArtifact) repository.getNode(dependency).getArtifact(), recursive);
						}
					}
					catch (Exception e) {
						logger.error("Could not start dependency: " + dependency, e);
					}
				}
			}
		}
		catch (Exception e) {
			logger.error("Error while starting " + artifact.getClass().getSimpleName() + ": " + artifact.getId(), e);
		}
	}
	
	public void restart(String id) {
		Artifact resolved = getRepository().resolve(id);
		if (resolved instanceof RestartableArtifact) {
			restart((RestartableArtifact) resolved, true);
		}
		else if (resolved instanceof StartableArtifact && resolved instanceof StoppableArtifact) {
			stop((StoppableArtifact) resolved, true);
			start((StartableArtifact) resolved, true);
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
			start((StartableArtifact) resolved, true);
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
								start((RestartableArtifact) repository.getNode(dependency).getArtifact(), recursive);
							}
						}
					}
					catch (Exception e) {
						logger.error("Could not restart dependency: " + dependency, e);
					}
				}
			}
		}
		catch (Exception e) {
			logger.error("Error while restarting " + artifact.getClass().getSimpleName() + ": " + artifact.getId(), e);
		}
	}
	
	private void stop(StoppableArtifact artifact, boolean recursive) {
		logger.info("Stopping " + artifact.getClass().getSimpleName() + ": " + artifact.getId());
		try {
			if (!(artifact instanceof StartableArtifact) || ((StartableArtifact) artifact).isStarted()) {
				artifact.stop();
			}
			if (recursive) {
				for (String dependency : repository.getDependencies(artifact.getId())) {
					try {
						if (repository.getNode(dependency).isLoaded() && repository.getNode(dependency).getArtifact() instanceof StoppableArtifact) {
							stop((StoppableArtifact) repository.getNode(dependency).getArtifact(), recursive);
						}
					}
					catch (Exception e) {
						logger.error("Could not stop dependency: " + dependency, e);
					}
				}
			}
		}
		catch (Exception e) {
			logger.error("Error while stopping " + artifact.getClass().getSimpleName() + ": " + artifact.getId(), e);
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
		runnable.run();
		return new ServiceResultFuture(new SimpleServiceResult(runnable.getOutput(), runnable.getException()));
	}
	
	static class ServiceResultFuture implements Future<ServiceResult> {
		
		private ServiceResult serviceResult;
		
		ServiceResultFuture(ServiceResult serviceResult) {
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
	
	public void start() throws IOException {
		startupTime = new Date();
		repository.start();
	}
	
	public URI getRepositoryRoot() {
		return ResourceUtils.getURI(((ResourceRepository) repository).getRoot().getContainer());
	}
	
	/**
	 * TODO: Only checks direct references atm, not recursive ones
	 */
	private static void orderNodes(final Repository repository, List<NodeEvent> events) {
		Collections.sort(events, new Comparator<NodeEvent>() {
			@Override
			public int compare(NodeEvent o1, NodeEvent o2) {
				List<String> references = repository.getReferences(o1.getId());
				return references == null || !references.contains(o2.getId()) ? 0 : 1;
			}
		});
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
}
