package be.nabu.eai.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import be.nabu.eai.repository.EAIResourceRepository;
import be.nabu.eai.repository.api.ArtifactManager;
import be.nabu.eai.repository.api.Node;
import be.nabu.eai.repository.api.ResourceEntry;
import be.nabu.eai.repository.events.NodeEvent;
import be.nabu.eai.repository.events.RepositoryEvent;
import be.nabu.eai.repository.resources.RepositoryEntry;
import be.nabu.libs.artifacts.api.Artifact;
import be.nabu.libs.artifacts.api.StartableArtifact;
import be.nabu.libs.artifacts.api.StoppableArtifact;
import be.nabu.libs.resources.memory.MemoryDirectory;
import be.nabu.libs.validator.api.Validation;

public class ReloadLifecycleTest {
	public static class Application implements StartableArtifact, StoppableArtifact {
		private final String id;
		private boolean started;
		private int starts;
		private int stops;

		public Application(String id) {
			this.id = id;
		}

		@Override
		public String getId() {
			return id;
		}

		@Override
		public void start() {
			started = true;
			starts++;
		}

		@Override
		public void stop() {
			started = false;
			stops++;
		}

		@Override
		public boolean isStarted() {
			return started;
		}
	}

	public static class ApplicationManager implements ArtifactManager<Application> {
		public static Application application;

		@Override
		public Application load(ResourceEntry entry, List<Validation<?>> messages) throws IOException, ParseException {
			return application;
		}

		@Override
		public List<Validation<?>> save(ResourceEntry entry, Application artifact) {
			return Collections.emptyList();
		}

		@Override
		public Class<Application> getArtifactClass() {
			return Application.class;
		}

		@Override
		public List<String> getReferences(Application artifact) {
			return Collections.emptyList();
		}

		@Override
		public List<Validation<?>> updateReference(Application artifact, String from, String to) {
			return Collections.emptyList();
		}
	}

	private static class Repository extends EAIResourceRepository {
		private boolean staleAtScan;
		private int scans;

		private Repository() throws IOException {
			super(new MemoryDirectory("repository"), new MemoryDirectory("maven"));
		}

		@Override
		public Map<Class<? extends Artifact>, Map<String, Node>> scanForTypes() {
			scans++;
			if (staleAtScan) {
				staleAtScan = false;
				((RepositoryEntry) getEntry("application")).refresh(false);
			}
			return super.scanForTypes();
		}
	}

	private void verifyReload(boolean collection) throws Exception {
		Repository repository = new Repository();
		Application application = new Application("application");
		ApplicationManager.application = application;
		try {
			repository.getRoot().createNode("application", new ApplicationManager(), false);
			List<String> events = new ArrayList<String>();
			repository.getEventDispatcher().subscribe(RepositoryEvent.class, event -> {
				if (event.getState() == RepositoryEvent.RepositoryState.RELOAD) {
					events.add(event.isDone() ? "complete" : "begin");
				}
				return null;
			});
			repository.getEventDispatcher().subscribe(NodeEvent.class, event -> {
				if (event.getId().equals("application") && event.getState() == NodeEvent.State.RELOAD) {
					events.add(event.isDone() ? "node-done" : "node-begin");
				}
				return null;
			});
			Server server = new Server(null, repository, null);
			server.initialize();
			Field started = Server.class.getDeclaredField("isStarted");
			started.setAccessible(true);
			started.set(server, true);
			Field reloadTime = Server.class.getDeclaredField("reloadTime");
			reloadTime.setAccessible(true);
			reloadTime.set(server, new Date());
			repository.getEventDispatcher().fire(new NodeEvent("application", repository.getNode("application"), NodeEvent.State.LOAD, true), repository);
			assertTrue(application.isStarted());
			int startsBefore = application.starts;
			repository.staleAtScan = true;
			if (collection) {
				repository.reloadAll(Arrays.asList("missing"));
			}
			else {
				repository.reload("missing");
			}
			assertEquals(1, repository.scans);
			assertEquals(1, application.stops);
			assertTrue("Application must be live after the final scan", application.isStarted());
			assertEquals(startsBefore + 1, application.starts);
			assertTrue(events.toString(), events.indexOf("begin") < events.indexOf("complete"));
			assertTrue(events.toString(), events.indexOf("complete") < events.indexOf("node-begin"));
			assertTrue(events.toString(), events.indexOf("node-begin") < events.indexOf("node-done"));
			Field loading = Server.class.getDeclaredField("isRepositoryLoading");
			loading.setAccessible(true);
			assertEquals(false, loading.get(server));
		}
		finally {
			ApplicationManager.application = null;
		}
	}

	public static class Host implements Artifact {
		private final String id;
		private final Object dispatcher = new Object();
		private final List<Object> registrations = new ArrayList<Object>();

		public Host(String id) {
			this.id = id;
		}

		@Override
		public String getId() {
			return id;
		}
	}

	public static class BoundApplication implements StartableArtifact, StoppableArtifact {
		private final String id;
		private final Host host;
		private boolean started;

		public BoundApplication(String id, Host host) {
			this.id = id;
			this.host = host;
		}

		@Override
		public String getId() {
			return id;
		}

		@Override
		public void start() {
			host.registrations.add(host.dispatcher);
			started = true;
		}

		@Override
		public void stop() {
			host.registrations.remove(host.dispatcher);
			started = false;
		}

		@Override
		public boolean isStarted() {
			return started;
		}
	}

	public static abstract class FixtureManager<T extends Artifact> implements ArtifactManager<T> {
		@Override
		public List<Validation<?>> save(ResourceEntry entry, T artifact) {
			return Collections.emptyList();
		}

		@Override
		public List<String> getReferences(T artifact) {
			return Collections.emptyList();
		}

		@Override
		public List<Validation<?>> updateReference(T artifact, String from, String to) {
			return Collections.emptyList();
		}
	}

	public static class HostManager extends FixtureManager<Host> {
		@Override
		public Host load(ResourceEntry entry, List<Validation<?>> messages) {
			return new Host(entry.getId());
		}

		@Override
		public Class<Host> getArtifactClass() {
			return Host.class;
		}
	}

	public static class BoundApplicationManager extends FixtureManager<BoundApplication> {
		@Override
		public BoundApplication load(ResourceEntry entry, List<Validation<?>> messages) {
			return new BoundApplication(entry.getId(), (Host) entry.getRepository().resolve("bHost"));
		}

		@Override
		public Class<BoundApplication> getArtifactClass() {
			return BoundApplication.class;
		}
	}

	private void verifyHostIdentity(boolean collection) throws Exception {
		Repository repository = new Repository();
		repository.getRoot().createNode("aApplication", new BoundApplicationManager(), false);
		repository.getRoot().createNode("bHost", new HostManager(), false);
		repository.getRoot().createNode("cServer", new HostManager(), false);
		((RepositoryEntry) repository.getEntry("aApplication")).updateNode(Arrays.asList("bHost"));
		((RepositoryEntry) repository.getEntry("bHost")).updateNode(Arrays.asList("cServer"));
		List<String> completed = new ArrayList<String>();
		repository.getEventDispatcher().subscribe(NodeEvent.class, event -> {
			if (event.getState() == NodeEvent.State.RELOAD && event.isDone()) {
				completed.add(event.getId());
			}
			return null;
		});
		Server server = new Server(null, repository, null);
		server.initialize();
		Field started = Server.class.getDeclaredField("isStarted");
		started.setAccessible(true);
		started.set(server, true);
		Field reloadTime = Server.class.getDeclaredField("reloadTime");
		reloadTime.setAccessible(true);
		reloadTime.set(server, new Date());
		repository.scanForTypes();
		BoundApplication initial = (BoundApplication) repository.resolve("aApplication");
		initial.start();
		Host previous = (Host) repository.resolve("bHost");
		if (collection) {
			repository.reloadAll(Arrays.asList("bHost", "cServer", "bHost"));
		}
		else {
			repository.reload("bHost");
		}
		BoundApplication current = (BoundApplication) repository.resolve("aApplication");
		Host host = (Host) repository.resolve("bHost");
		assertTrue("Host must have been replaced", previous != host);
		assertSame("Application must retain the current host", host, current.host);
		assertTrue("Application must be started", current.isStarted());
		assertEquals(1, host.registrations.size());
		assertSame("Registration must use current dispatcher", host.dispatcher, host.registrations.get(0));
		assertEquals("Application reload must not be duplicated: " + completed, 1, Collections.frequency(completed, "aApplication"));
	}

	@Test
	public void recursiveHostReloadRetainsCurrentHost() throws Exception {
		verifyHostIdentity(false);
	}

	@Test
	public void overlappingCollectionReloadRetainsCurrentHost() throws Exception {
		verifyHostIdentity(true);
	}

	@Test
	public void collectionReloadDeduplicatesRealNodesAndScansOnce() throws Exception {
		Repository repository = new Repository();
		Application application = new Application("application");
		ApplicationManager.application = application;
		try {
			repository.getRoot().createNode("application", new ApplicationManager(), false);
			repository.getRoot().createNode("other", new ApplicationManager(), false);
			List<String> events = new ArrayList<String>();
			repository.getEventDispatcher().subscribe(NodeEvent.class, event -> {
				if (event.getState() == NodeEvent.State.RELOAD && event.isDone()) {
					events.add(event.getId());
				}
				return null;
			});
			repository.reloadAll(Arrays.asList("application", "other", "application"));
			assertEquals(Arrays.asList("application", "other"), events);
			assertEquals(1, repository.scans);
			assertTrue(repository.getNode("application") != null);
		}
		finally {
			ApplicationManager.application = null;
		}
	}

	@Test
	public void collectionReloadRestartsStaleNodeFoundInFinalScan() throws Exception {
		verifyReload(true);
	}

	@Test
	public void recursiveReloadRestartsStaleNodeFoundInFinalScan() throws Exception {
		verifyReload(false);
	}
}
