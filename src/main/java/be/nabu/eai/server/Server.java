package be.nabu.eai.server;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.eai.repository.api.MavenRepository;
import be.nabu.eai.repository.api.Repository;
import be.nabu.eai.repository.events.NodeEvent;
import be.nabu.eai.repository.events.RepositoryEvent;
import be.nabu.eai.repository.events.RepositoryEvent.RepositoryState;
import be.nabu.eai.repository.util.NodeUtils;
import be.nabu.eai.repository.util.SystemPrincipal;
import be.nabu.eai.server.rest.ServerREST;
import be.nabu.libs.artifacts.api.Artifact;
import be.nabu.libs.artifacts.api.RestartableArtifact;
import be.nabu.libs.artifacts.api.StartableArtifact;
import be.nabu.libs.artifacts.api.StoppableArtifact;
import be.nabu.libs.authentication.api.RoleHandler;
import be.nabu.libs.cache.api.CacheProvider;
import be.nabu.libs.events.api.EventHandler;
import be.nabu.libs.http.api.HTTPRequest;
import be.nabu.libs.http.api.server.HTTPServer;
import be.nabu.libs.http.server.rest.RESTHandler;
import be.nabu.libs.maven.CreateResourceRepositoryEvent;
import be.nabu.libs.maven.DeleteResourceRepositoryEvent;
import be.nabu.libs.maven.MavenListener;
import be.nabu.libs.resources.ResourceUtils;
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
import be.nabu.libs.services.api.ServiceRuntimeTracker;
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
	private CacheProvider cacheProvider;
	
	/**
	 * This is set to true while the repository is loading
	 * This allows us to queue actions (in the delayedArtifacts) to be done after the repository is done loading
	 * For example if we have an artifact that has to start(), it might depend on another artifact, so we have to finish loading first
	 */
	private boolean isRepositoryLoading = false;
	private List<NodeEvent> delayedNodeEvents = new ArrayList<NodeEvent>();
	
	public Server(RoleHandler roleHandler, MavenRepository repository) throws IOException {
		this.roleHandler = roleHandler;
		this.repository = repository;
		initialize();
	}

	public void enableREST(HTTPServer server) {
		// make sure we intercept invoke commands
		server.getDispatcher(null).subscribe(HTTPRequest.class, new RESTHandler("/", ServerREST.class, roleHandler, repository, this));
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
								start(nodeEvent);
							}
						}
					}
					else if (nodeEvent.getState() == NodeEvent.State.RELOAD) {
						try {
							if (RestartableArtifact.class.isAssignableFrom(nodeEvent.getNode().getArtifactClass())) {
								restart((RestartableArtifact) nodeEvent.getNode().getArtifact());
							}
							else if (StartableArtifact.class.isAssignableFrom(nodeEvent.getNode().getArtifactClass())) {
								start((StartableArtifact) nodeEvent.getNode().getArtifact());
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
							if (StoppableArtifact.class.isAssignableFrom(nodeEvent.getNode().getArtifactClass())) {
								stop((StoppableArtifact) nodeEvent.getNode().getArtifact());
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
				if (event.getState() == RepositoryState.LOAD) {
					// if the loading is done, toggle the boolean and finish delayed actions
					if (event.isDone()) {
						logger.info("Repository loaded, processing nodes");
						isRepositoryLoading = false;
						orderNodes(repository, delayedNodeEvents);
						// TODO: load in dependency order!
						for (NodeEvent delayedNodeEvent : delayedNodeEvents) {
							if (StartableArtifact.class.isAssignableFrom(delayedNodeEvent.getNode().getArtifactClass())) {
								start(delayedNodeEvent);
							}
							if (DefinedService.class.isAssignableFrom(delayedNodeEvent.getNode().getArtifactClass()) && NodeUtils.isEager(delayedNodeEvent.getNode())) {
								run(delayedNodeEvent);
							}
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
				((DefinedService) nodeEvent.getNode().getArtifact()),
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
	
	private void start(NodeEvent nodeEvent) {
		try {
			start((StartableArtifact) nodeEvent.getNode().getArtifact());
		}
		catch (IOException e) {
			logger.error("Failed to load artifact: " + nodeEvent.getId(), e);
		}
		catch (ParseException e) {
			logger.error("Failed to load artifact: " + nodeEvent.getId(), e);
		}
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
	
	private void start(StartableArtifact artifact) {
		logger.info("Starting " + artifact.getClass().getSimpleName() + ": " + artifact.getId());
		try {
			artifact.start();
		}
		catch (Exception e) {
			logger.error("Error while starting " + artifact.getClass().getSimpleName() + ": " + artifact.getId(), e);
		}
	}
	
	public void restart(String id) {
		Artifact resolved = getRepository().resolve(id);
		if (resolved instanceof RestartableArtifact) {
			restart((RestartableArtifact) resolved);
		}
		else if (resolved instanceof StartableArtifact && resolved instanceof StoppableArtifact) {
			stop((StoppableArtifact) resolved);
			start((StartableArtifact) resolved);
		}
	}
	
	public void stop(String id) {
		Artifact resolved = getRepository().resolve(id);
		if (resolved instanceof StoppableArtifact) {
			stop((StoppableArtifact) resolved);
		}
	}
	
	public void start(String id) {
		Artifact resolved = getRepository().resolve(id);
		if (resolved instanceof StartableArtifact) {
			start((StartableArtifact) resolved);
		}
	}
	
	private void restart(RestartableArtifact artifact) {
		logger.info("Restarting " + artifact.getClass().getSimpleName() + ": " + artifact.getId());
		try {
			artifact.restart();
		}
		catch (Exception e) {
			logger.error("Error while restarting " + artifact.getClass().getSimpleName() + ": " + artifact.getId(), e);
		}
	}
	
	private void stop(StoppableArtifact artifact) {
		logger.info("Stopping " + artifact.getClass().getSimpleName() + ": " + artifact.getId());
		try {
			artifact.stop();
		}
		catch (Exception e) {
			logger.error("Error while stopping " + artifact.getClass().getSimpleName() + ": " + artifact.getId(), e);
		}
	}

	public MavenRepository getRepository() {
		return repository;
	}

	@Override
	public Future<ServiceResult> run(Service service, ExecutionContext executionContext, ComplexContent input, ServiceRuntimeTracker tracker, ServiceRunnableObserver...observers) {
		List<ServiceRunnableObserver> allObservers = new ArrayList<ServiceRunnableObserver>(observers.length + 1);
		allObservers.add(new RunningServiceObserver());
		allObservers.addAll(Arrays.asList(observers));
		ServiceRuntime serviceRuntime = new ServiceRuntime(service, executionContext);
		serviceRuntime.setRuntimeTracker(tracker);
		serviceRuntime.setCache(cacheProvider);
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
		repository.start();
	}
	
	public URI getRepositoryRoot() {
		return ResourceUtils.getURI(repository.getRoot().getContainer());
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

	@Override
	public CacheProvider getCacheProvider() {
		return cacheProvider;
	}

	@Override
	public void setCacheProvider(CacheProvider cacheProvider) {
		this.cacheProvider = cacheProvider;
	}
}
