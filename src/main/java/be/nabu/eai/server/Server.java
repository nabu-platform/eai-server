package be.nabu.eai.server;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.eai.repository.EAIResourceRepository;
import be.nabu.eai.repository.api.Repository;
import be.nabu.eai.repository.events.NodeEvent;
import be.nabu.eai.repository.managers.MavenManager;
import be.nabu.eai.server.rest.ServerREST;
import be.nabu.libs.artifacts.api.RestartableArtifact;
import be.nabu.libs.artifacts.api.StartableArtifact;
import be.nabu.libs.artifacts.api.StoppableArtifact;
import be.nabu.libs.events.api.EventHandler;
import be.nabu.libs.maven.CreateResourceRepositoryEvent;
import be.nabu.libs.maven.DeleteResourceRepositoryEvent;
import be.nabu.libs.maven.MavenListener;
import be.nabu.libs.resources.ResourceUtils;
import be.nabu.libs.resources.api.ResourceContainer;
import be.nabu.libs.services.ServiceRunnable;
import be.nabu.libs.services.ServiceRuntime;
import be.nabu.libs.services.SimpleServiceResult;
import be.nabu.libs.services.api.ExecutionContext;
import be.nabu.libs.services.api.Service;
import be.nabu.libs.services.api.ServiceResult;
import be.nabu.libs.services.api.ServiceRunnableObserver;
import be.nabu.libs.services.api.ServiceRunner;
import be.nabu.libs.services.maven.MavenArtifact;
import be.nabu.libs.types.DefinedTypeResolverFactory;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.utils.http.api.HTTPRequest;
import be.nabu.utils.http.api.server.HTTPServer;
import be.nabu.utils.http.rest.RESTHandler;
import be.nabu.utils.http.rest.RoleHandler;

public class Server implements ServiceRunner {
	
	public static final String SERVICE_THREAD_POOL = "be.nabu.eai.server.serviceThreadPoolSize";
	
	private EAIResourceRepository repository;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private List<ServiceRunnable> runningServices = new ArrayList<ServiceRunnable>();

	private RoleHandler roleHandler;
	
	public Server(RoleHandler roleHandler, ResourceContainer<?> repositoryRoot, ResourceContainer<?> mavenRoot) throws IOException {
		this.roleHandler = roleHandler;
		this.repository = new EAIResourceRepository(repositoryRoot, mavenRoot);
		this.repository.setServiceRunner(this);
		initialize();
	}

	public void enableREST(HTTPServer server) {
		// make sure we intercept invoke commands
		server.getEventDispatcher().subscribe(HTTPRequest.class, new RESTHandler("/", ServerREST.class, roleHandler, repository, this));
	}
	
	public void initialize() {
		// make sure we respond to repository events
		repository.getEventDispatcher().subscribe(NodeEvent.class, new EventHandler<NodeEvent, Void>() {
			@Override
			public Void handle(NodeEvent nodeEvent) {
				if (nodeEvent.isDone()) {
					// a new node is loaded, let's check if we have to set something up
					if (nodeEvent.getState() == NodeEvent.State.LOAD) {
						try {
							if (StartableArtifact.class.isAssignableFrom(nodeEvent.getNode().getArtifactClass())) {
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
					else if (nodeEvent.getState() == NodeEvent.State.RELOAD) {
						try {
							if (RestartableArtifact.class.isAssignableFrom(nodeEvent.getNode().getArtifactClass())) {
								restart((RestartableArtifact) nodeEvent.getNode().getArtifact());
							}
							else {
								if (StoppableArtifact.class.isAssignableFrom(nodeEvent.getNode().getArtifactClass())) {
									stop((StoppableArtifact) nodeEvent.getNode().getArtifact());
								}
								if (StartableArtifact.class.isAssignableFrom(nodeEvent.getNode().getArtifactClass())) {
									start((StartableArtifact) nodeEvent.getNode().getArtifact());
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
				else {
					// if a node is unloaded we might need to stop something
					if (nodeEvent.getState() == NodeEvent.State.UNLOAD) {
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
	}
	
	public void enableMaven(HTTPServer server) {
		repository.getEventDispatcher().subscribe(DeleteResourceRepositoryEvent.class, new EventHandler<DeleteResourceRepositoryEvent, Void>() {
			@Override
			public Void handle(DeleteResourceRepositoryEvent event) {
				logger.info("Deleting maven artifact " + event.getArtifact().getArtifactId());
				MavenManager manager = new MavenManager(DefinedTypeResolverFactory.getInstance().getResolver());
				try {
					manager.removeChildren(repository.getRoot(), manager.load(repository.getMavenRepository(), event.getArtifact(), repository.getLocalMavenServer(), repository.isUpdateMavenSnapshots()));
				}
				catch (IOException e) {
					throw new RuntimeException(e);
				}
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
				MavenManager manager = new MavenManager(DefinedTypeResolverFactory.getInstance().getResolver());
				MavenArtifact artifact = manager.load(repository.getMavenRepository(), event.getArtifact(), repository.getLocalMavenServer(), repository.isUpdateMavenSnapshots());
				try {
					manager.addChildren(repository.getRoot(), artifact);
				}
				catch (IOException e) {
					throw new RuntimeException(e);
				}
				return null;
			}
		}).filter(new EventHandler<CreateResourceRepositoryEvent, Boolean>() {
			@Override
			public Boolean handle(CreateResourceRepositoryEvent event) {
				return !event.isInternal();
			}
		});
		// no support for non-root calls atm!
		server.getEventDispatcher().subscribe(HTTPRequest.class, new MavenListener(repository.getMavenRepository(), "maven"));
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

	public Repository getRepository() {
		return repository;
	}

	@Override
	public Future<ServiceResult> run(Service service, ExecutionContext executionContext, ComplexContent input, ServiceRunnableObserver...observers) {
		List<ServiceRunnableObserver> allObservers = new ArrayList<ServiceRunnableObserver>(observers.length + 1);
		allObservers.add(new RunningServiceObserver());
		allObservers.addAll(Arrays.asList(observers));
		final ServiceRunnable runnable = new ServiceRunnable(new ServiceRuntime(service, executionContext), input, allObservers.toArray(new ServiceRunnableObserver[allObservers.size()]));
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
	
	public URI getMavenRoot() {
		return ResourceUtils.getURI(repository.getMavenRoot());
	}
	
	public void setLocalMavenServer(URI uri) {
		repository.setLocalMavenServer(uri);
	}
	
	public void setUpdateMavenSnapshots(boolean update) {
		repository.setUpdateMavenSnapshots(update);
	}
}
