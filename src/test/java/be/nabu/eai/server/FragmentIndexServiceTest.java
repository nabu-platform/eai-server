package be.nabu.eai.server;

import java.lang.reflect.Proxy;
import java.net.URL;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import be.nabu.eai.repository.api.ArtifactFragmentManager;
import be.nabu.eai.repository.api.ArtifactFragmentManager.ArtifactFragment;
import be.nabu.eai.repository.api.ArtifactFragmentManager.RevisionedArtifactFragment;
import be.nabu.eai.repository.api.Entry;
import be.nabu.eai.repository.api.Node;
import be.nabu.eai.repository.api.Repository;
import be.nabu.eai.repository.events.NodeEvent;
import be.nabu.eai.repository.events.RepositoryEvent;
import be.nabu.eai.server.fragments.FileSystemFragmentIndexBackend;
import be.nabu.eai.server.fragments.FragmentIndexService;
import be.nabu.libs.artifacts.api.Artifact;
import be.nabu.libs.validator.api.Validation;

public class FragmentIndexServiceTest {
	public static class Item implements Artifact {
		public String getId() { return "app"; }
	}

	public static class Manager implements ArtifactFragmentManager<Item> {
		static final List<Piece> pieces = new ArrayList<Piece>();
		static String category = "original";
		public String getArtifactType() { return "item"; }
		public String getArtifactCategory() { return category; }
		public List<ArtifactFragment> listFragments(Item item) { return new ArrayList<ArtifactFragment>(pieces); }
		public List<Validation<?>> updateFragment(Item item, String path, String oldContent, String newContent) { return Collections.emptyList(); }
		public Class<Item> getArtifactClass() { return Item.class; }
	}

	private static class Piece implements RevisionedArtifactFragment {
		String path, content, revision;
		boolean editable;
		int reads;
		final Map<String, String> properties = new LinkedHashMap<String, String>();
		Piece(String path, String content, String revision) { this.path = path; this.content = content; this.revision = revision; }
		public String getPath() { return path; }
		public String getRevision() { return revision; }
		public String getContent() { reads++; return content; }
		public String getContentType() { return "text/plain"; }
		public String getArtifactId() { return "app"; }
		public String getFragmentType() { return "resource"; }
		public Map<String, String> getProperties() { return properties; }
		public boolean isEditable() { return editable; }
		public boolean isRemovable() { return false; }
	}

	private static class CountingBackend extends FileSystemFragmentIndexBackend {
		int writes, deletes, calls, registryWrites;
		boolean fail, failRegistry;
		Runnable onIndex;
		CountingBackend(Path root) { super(root); }
		@Override
		public void index(String id, String type, String category, long version, List<ArtifactFragment> changed, List<String> paths) {
			calls++;
			writes += changed.size();
			if (fail) { throw new IllegalStateException("test failure"); }
			super.index(id, type, category, version, changed, paths);
			if (onIndex != null) {
				Runnable callback = onIndex;
				onIndex = null;
				callback.run();
			}
		}
		@Override
		protected void writeRegistry(Path path, Map<String, String> values) throws IOException {
			registryWrites++;
			if (failRegistry) { throw new IOException("test registry failure"); }
			super.writeRegistry(path, values);
		}
		@Override
		public void delete(String id) { deletes++; super.delete(id); }
	}

	private ClassLoader originalLoader;
	private Map<String, Node> nodes;
	private FragmentIndexService service;
	private CountingBackend backend;
	private Path indexRoot;
	private Repository repository;
	private int rootReads;
	private final Map<String, Integer> nodeReads = new LinkedHashMap<String, Integer>();
	private String forbiddenId;
	private Runnable onNodeRead;

	@Before
	public void setUp() throws Exception {
		originalLoader = Thread.currentThread().getContextClassLoader();
		Path services = Files.createTempFile("fragment-manager", ".txt");
		Files.write(services, Collections.singletonList(Manager.class.getName()), StandardCharsets.UTF_8);
		URL url = services.toUri().toURL();
		Thread.currentThread().setContextClassLoader(new ClassLoader(originalLoader) {
			@Override
			public Enumeration<URL> getResources(String name) throws java.io.IOException {
				if (name.equals("META-INF/services/" + ArtifactFragmentManager.class.getName())) {
					return Collections.enumeration(Collections.singletonList(url));
				}
				return super.getResources(name);
			}
		});
		Manager.pieces.clear();
		Manager.category = "original";
		nodes = new LinkedHashMap<String, Node>();
		Node node = (Node) Proxy.newProxyInstance(originalLoader, new Class<?>[] { Node.class }, (proxy, method, args) -> {
			if (method.getName().equals("getArtifact")) { return new Item(); }
			if (method.getName().equals("getVersion")) { return 1L; }
			return null;
		});
		nodes.put("app", node);
		Entry root = (Entry) Proxy.newProxyInstance(originalLoader, new Class<?>[] { Entry.class }, (proxy, method, args) -> {
			if (method.getName().equals("iterator")) {
				List<Entry> entries = new ArrayList<Entry>();
				for (String id : nodes.keySet()) {
					entries.add((Entry) Proxy.newProxyInstance(originalLoader, new Class<?>[] { Entry.class }, (entry, call, parameters) -> {
						if (call.getName().equals("isNode")) { return true; }
						if (call.getName().equals("getId")) { return id; }
						if (call.getName().equals("getNode")) { return nodes.get(id); }
						if (call.getName().equals("iterator")) { return Collections.emptyIterator(); }
						return null;
					}));
				}
				return entries.iterator();
			}
			if (method.getName().equals("isNode")) { return false; }
			return null;
		});
		repository = (Repository) Proxy.newProxyInstance(originalLoader, new Class<?>[] { Repository.class }, (proxy, method, args) -> {
			if (method.getName().equals("getRoot")) { rootReads++; return root; }
			if (method.getName().equals("getNode")) {
				String id = (String) args[0];
				if (id.equals(forbiddenId)) { throw new AssertionError("Unrelated node resolved: " + id); }
				nodeReads.put(id, nodeReads.containsKey(id) ? nodeReads.get(id) + 1 : 1);
				if (onNodeRead != null) {
					Runnable callback = onNodeRead;
					onNodeRead = null;
					callback.run();
				}
				return nodes.get(id);
			}
			return null;
		});
		indexRoot = Files.createTempDirectory("fragment-index-service");
		backend = new CountingBackend(indexRoot);
		service = new FragmentIndexService(repository, backend);
		service.initialize();
	}

	@After
	public void tearDown() { Thread.currentThread().setContextClassLoader(originalLoader); Manager.pieces.clear(); }

	@Test
	public void lazyRevisionsAndFallbackAndMetadata() {
		for (int i = 0; i < 220; i++) { Manager.pieces.add(new Piece("file" + i, "value" + i, "1")); }
		service.rebuild();
		assertEquals(220, backend.writes);
		assertEquals(1, backend.registryWrites);
		service.refresh("app");
		assertEquals(220, backend.writes);
		assertEquals(1, Manager.pieces.get(0).reads);
		Piece edited = Manager.pieces.get(4);
		edited.content = "edited"; edited.revision = "2";
		service.refresh("app");
		assertEquals(221, backend.writes);
		assertEquals(2, backend.registryWrites);
		assertEquals(2, edited.reads);
		assertEquals(1, Manager.pieces.get(5).reads);
		edited.editable = true;
		edited.properties.put("key", "value");
		service.refresh("app");
		assertEquals(222, backend.writes);
		assertTrue(backend.get("app", edited.path).isEditable());
		assertEquals("value", backend.get("app", edited.path).getProperties().get("key"));
		edited.revision = null; edited.content = "fallback";
		service.refresh("app");
		assertEquals(223, backend.writes);
		service.refresh("app");
		assertEquals(223, backend.writes);
		Manager.category = "updated";
		service.refresh("app");
		assertEquals(443, backend.writes);
		assertEquals("updated", backend.get("app", "file5").getArtifactCategory());
	}

	@Test
	public void manifestFailureRetryAndRebuild() {
		Piece first = new Piece("old", "before", "1");
		Manager.pieces.add(first);
		service.rebuild();
		first.path = "new";
		backend.fail = true;
		service.refresh("app");
		assertNotNull(backend.get("app", "old"));
		assertEquals(1, backend.registryWrites);
		backend.fail = false;
		service.refresh("app");
		assertNull(backend.get("app", "old"));
		assertNotNull(backend.get("app", "new"));
		Manager.pieces.clear();
		service.rebuild();
		assertNull(backend.get("app", "new"));
		Manager.pieces.add(first);
		service.rebuild();
		assertNotNull(backend.get("app", "new"));
	}

	@Test
	public void failedRebuildDoesNotPublishPartialManifest() {
		Piece first = new Piece("original", "before", "1");
		Manager.pieces.add(first);
		service.rebuild();
		first.path = "replacement";
		backend.fail = true;
		try {
			service.rebuild();
			fail("Expected incomplete rebuild");
		}
		catch (IllegalStateException expected) {
			assertNotNull(backend.get("app", "original"));
		}
		backend.fail = false;
		service.rebuild();
		assertNull(backend.get("app", "original"));
		assertNotNull(backend.get("app", "replacement"));
	}

	@Test
	public void revisionChangesDuringIndexAreRetried() {
		Piece changing = new Piece("changing", "first", "1") {
			@Override
			public String getContent() { String result = super.getContent(); revision = "2"; return result; }
		};
		Manager.pieces.add(changing);
		service.refresh("app");
		assertNull(backend.get("app", "changing"));
		service.refresh("app");
		assertNotNull(backend.get("app", "changing"));
	}

	@Test
	public void scopedBatchAndNestedCompletion() {
		Manager.pieces.add(new Piece("path", "value", "1"));
		nodes.put("unrelated", nodes.get("app"));
		service.rebuild();
		int roots = rootReads;
		int reads = Manager.pieces.get(0).reads;
		forbiddenId = "unrelated";
		service.handle(new RepositoryEvent(RepositoryEvent.RepositoryState.RELOAD, false));
		service.handle(new RepositoryEvent(RepositoryEvent.RepositoryState.RELOAD, false));
		for (int i = 0; i < 3; i++) {
			service.handle(new NodeEvent("app", nodes.get("app"), NodeEvent.State.UNLOAD, true));
			service.handle(new NodeEvent("app", nodes.get("app"), NodeEvent.State.LOAD, true));
		}
		service.handle(new RepositoryEvent(RepositoryEvent.RepositoryState.RELOAD, true));
		assertEquals(0, nodeReads.size());
		service.handle(new RepositoryEvent(RepositoryEvent.RepositoryState.RELOAD, true));
		assertEquals(Integer.valueOf(1), nodeReads.get("app"));
		assertEquals(roots, rootReads);
		assertEquals(reads, Manager.pieces.get(0).reads);
		assertEquals(0, backend.deletes);
		forbiddenId = null;
		service.rebuild();
		assertEquals(roots + 1, rootReads);
	}

	@Test
	public void removedGeneratedChildAndFailedBatchRetry() {
		Manager.pieces.add(new Piece("path", "value", "1"));
		nodes.put("app.generated", nodes.get("app"));
		service.rebuild();
		int roots = rootReads;
		nodes.remove("app.generated");
		service.handle(new RepositoryEvent(RepositoryEvent.RepositoryState.RELOAD, false));
		service.handle(new NodeEvent("app.generated", null, NodeEvent.State.DELETE, true));
		service.handle(new RepositoryEvent(RepositoryEvent.RepositoryState.RELOAD, true));
		assertNull(backend.get("app.generated", "path"));
		assertNotNull(backend.get("app", "path"));
		assertEquals(roots, rootReads);
		backend.fail = true;
		Manager.pieces.get(0).revision = "2";
		service.handle(new RepositoryEvent(RepositoryEvent.RepositoryState.RELOAD, false));
		service.handle(new NodeEvent("app", nodes.get("app"), NodeEvent.State.RELOAD, true));
		service.handle(new RepositoryEvent(RepositoryEvent.RepositoryState.RELOAD, true));
		assertNotNull(backend.get("app", "path"));
		backend.fail = false;
		int calls = backend.calls;
		service.handle(new RepositoryEvent(RepositoryEvent.RepositoryState.RELOAD, true));
		assertEquals(calls + 1, backend.calls);
		assertEquals("2", Manager.pieces.get(0).revision);
		assertEquals(roots, rootReads);
	}

	@Test
	public void failedRetryDoesNotRunOnUnrelatedNodeEvent() {
		Manager.pieces.add(new Piece("path", "value", "1"));
		service.rebuild();
		Manager.pieces.get(0).revision = "2";
		backend.fail = true;
		service.handle(new NodeEvent("app", nodes.get("app"), NodeEvent.State.SAVE, true));
		int calls = backend.calls;
		backend.fail = false;
		service.handle(new NodeEvent("absent", null, NodeEvent.State.LOAD, true));
		assertEquals(calls, backend.calls);
		service.handle(new RepositoryEvent(RepositoryEvent.RepositoryState.RELOAD, true));
		assertEquals(calls + 1, backend.calls);
	}

	@Test
	public void reentrantEventsDrainWithoutRepeatingResolvedId() {
		Manager.pieces.add(new Piece("path", "value", "1"));
		service.rebuild();
		onNodeRead = () -> {
			service.handle(new NodeEvent("app", nodes.get("app"), NodeEvent.State.LOAD, true));
			service.handle(new NodeEvent("app.generated", null, NodeEvent.State.DELETE, true));
		};
		service.handle(new RepositoryEvent(RepositoryEvent.RepositoryState.RELOAD, false));
		service.handle(new NodeEvent("app", nodes.get("app"), NodeEvent.State.RELOAD, true));
		service.handle(new RepositoryEvent(RepositoryEvent.RepositoryState.RELOAD, true));
		assertEquals(Integer.valueOf(2), nodeReads.get("app"));
		assertEquals(Integer.valueOf(1), nodeReads.get("app.generated"));
		service.handle(new RepositoryEvent(RepositoryEvent.RepositoryState.RELOAD, true));
		assertEquals(Integer.valueOf(2), nodeReads.get("app"));
	}

	@Test
	public void sameIdMutationDuringIndexIsDrained() {
		Piece piece = new Piece("path", "before", "1");
		Manager.pieces.add(piece);
		backend.onIndex = () -> {
			piece.content = "after";
			piece.revision = "2";
			service.handle(new NodeEvent("app", nodes.get("app"), NodeEvent.State.SAVE, true));
		};
		service.handle(new NodeEvent("app", nodes.get("app"), NodeEvent.State.LOAD, true));
		assertEquals("after", backend.get("app", "path").getContent());
		assertEquals(2, backend.calls);
		assertEquals(Integer.valueOf(2), nodeReads.get("app"));
	}

	@Test
	public void nestedReloadDefersRemainingBatch() {
		Manager.pieces.add(new Piece("path", "value", "1"));
		nodes.put("second", nodes.get("app"));
		service.handle(new RepositoryEvent(RepositoryEvent.RepositoryState.RELOAD, false));
		service.handle(new NodeEvent("app", nodes.get("app"), NodeEvent.State.LOAD, true));
		service.handle(new NodeEvent("second", nodes.get("second"), NodeEvent.State.LOAD, true));
		onNodeRead = () -> service.handle(new RepositoryEvent(RepositoryEvent.RepositoryState.RELOAD, false));
		service.handle(new RepositoryEvent(RepositoryEvent.RepositoryState.RELOAD, true));
		assertNull(nodeReads.get("second"));
		service.handle(new RepositoryEvent(RepositoryEvent.RepositoryState.RELOAD, true));
		assertEquals(Integer.valueOf(1), nodeReads.get("second"));
	}

	@Test
	public void absentDeleteDoesNotRewriteRegistry() {
		Manager.pieces.add(new Piece("path", "value", "1"));
		service.rebuild();
		int writes = backend.registryWrites;
		backend.delete("absent");
		assertEquals(writes, backend.registryWrites);
		backend.delete("app");
		assertEquals(writes + 1, backend.registryWrites);
		backend.delete("app");
		assertEquals(writes + 1, backend.registryWrites);
	}

	@Test
	public void failedDeleteRegistryWriteIsRetried() throws Exception {
		Manager.pieces.add(new Piece("path", "value", "1"));
		service.rebuild();
		Path registry = indexRoot.resolve("artifact-index.properties");
		backend.failRegistry = true;
		try {
			backend.delete("app");
			fail("Expected registry failure");
		}
		catch (RuntimeException expected) {
			assertTrue(expected.getCause() instanceof IOException);
		}
		assertTrue(Files.readString(registry).contains("fragment.app"));
		backend.failRegistry = false;
		backend.delete("app");
		assertFalse(Files.readString(registry).contains("fragment.app"));
	}

	@Test
	public void reloadRetentionAndRealUnload() {
		Manager.pieces.add(new Piece("path", "value", "1"));
		service.rebuild();
		service.handle(new RepositoryEvent(RepositoryEvent.RepositoryState.RELOAD, false));
		service.handle(new NodeEvent("app", nodes.get("app"), NodeEvent.State.UNLOAD, true));
		assertNotNull(backend.get("app", "path"));
		service.handle(new RepositoryEvent(RepositoryEvent.RepositoryState.RELOAD, true));
		assertEquals(0, backend.deletes);
		service.handle(new NodeEvent("app", nodes.get("app"), NodeEvent.State.UNLOAD, true));
		assertNull(backend.get("app", "path"));
		service.refresh("app");
		nodes.clear();
		service.handle(new RepositoryEvent(RepositoryEvent.RepositoryState.RELOAD, false));
		service.handle(new NodeEvent("app", null, NodeEvent.State.DELETE, true));
		service.handle(new RepositoryEvent(RepositoryEvent.RepositoryState.RELOAD, true));
		assertNull(backend.get("app", "path"));
	}
}
