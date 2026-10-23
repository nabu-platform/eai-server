package be.nabu.eai.server;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import be.nabu.eai.repository.api.ArtifactFragmentManager.ArtifactFragment;
import be.nabu.eai.server.fragments.FileSystemFragmentIndexBackend;
import junit.framework.TestCase;

public class FragmentIndexBackendTest extends TestCase {
	public void testManifestRetainsUnchangedPathsAndUpdatesMetadata() throws Exception {
		Path root = Files.createTempDirectory("fragment-index-test");
		FileSystemFragmentIndexBackend backend = new FileSystemFragmentIndexBackend(root);
		backend.initialize();
		backend.index("app", "web", "category", 1, Arrays.asList(fragment("first", "a", false), fragment("second", "b", false)), Arrays.asList("first", "second"));
		Path first = root.resolve("app/first");
		long modified = Files.getLastModifiedTime(first).toMillis();
		backend.index("app", "web", "category", 2, Collections.singletonList(fragment("first", "a", true)), Arrays.asList("first", "second"));
		assertEquals(modified, Files.getLastModifiedTime(first).toMillis());
		assertTrue(backend.get("app", "first").isEditable());
		assertNotNull(backend.get("app", "second"));
		backend.index("app", "web", "category", 3, Collections.<ArtifactFragment>emptyList(), Collections.singletonList("first"));
		assertNull(backend.get("app", "second"));
		FileSystemFragmentIndexBackend restarted = new FileSystemFragmentIndexBackend(root);
		restarted.initialize();
		restarted.beginRebuild();
		restarted.index("app", "web", "category", 3, Collections.<ArtifactFragment>emptyList(), Collections.singletonList("first"));
		restarted.finalizeRebuild();
		assertNotNull(restarted.get("app", "first"));
	}

	private ArtifactFragment fragment(final String path, final String content, final boolean editable) {
		return new ArtifactFragment() {
			public String getPath() { return path; }
			public String getContent() { return content; }
			public String getContentType() { return "text/plain"; }
			public String getArtifactId() { return "app"; }
			public String getFragmentType() { return "resource"; }
			public Map<String, String> getProperties() { return Collections.emptyMap(); }
			public boolean isEditable() { return editable; }
			public boolean isRemovable() { return false; }
		};
	}
}
