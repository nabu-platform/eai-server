package be.nabu.eai.server.fragments;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.concurrent.TimeUnit;
import be.nabu.eai.repository.api.ArtifactFragmentManager.ArtifactFragment;
import be.nabu.eai.repository.api.ArtifactFragmentManager.RevisionedArtifactFragment;
import be.nabu.eai.repository.events.RepositoryEvent;
import be.nabu.eai.repository.events.RepositoryEvent.RepositoryState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.eai.repository.EAIRepositoryUtils;
import be.nabu.eai.repository.api.ArtifactFragmentManager;
import be.nabu.eai.repository.api.Entry;
import be.nabu.eai.repository.api.Node;
import be.nabu.eai.repository.api.Repository;
import be.nabu.eai.repository.events.NodeEvent;
import be.nabu.libs.artifacts.api.Artifact;

public class FragmentIndexService {

	private Logger logger = LoggerFactory.getLogger(getClass());
	private Repository repository;
	private FragmentIndexBackend backend;
	private final Map<String, Map<String, Snapshot>> baseline = new LinkedHashMap<String, Map<String, Snapshot>>();
	private int reloadDepth;
	private final Set<String> pending = new LinkedHashSet<String>();
	private final Set<String> retryPending = new LinkedHashSet<String>();
	private boolean reconciling;
	private boolean rebuildFailed;
	private boolean rebuilding;
	private long rebuildEntries;
	private String rebuildStage;
	private String rebuildId;

	public FragmentIndexService(Repository repository, FragmentIndexBackend backend) {
		this.repository = repository;
		this.backend = backend;
	}

	public void initialize() {
		backend.initialize();
	}

	public synchronized void rebuild() {
		baseline.clear();
		rebuildFailed = false;
		rebuilding = true;
		rebuildEntries = 0;
		rebuildId = null;
		rebuildStage = "beginRebuild";
		boolean diagnostics = Boolean.getBoolean("fragments.startup.diagnostics");
		long started = System.nanoTime();
		if (diagnostics) {
			logger.info("Fragment index rebuild starting");
		}
		backend.beginRebuild();
		try {
			rebuildStage = "getRoot";
			Entry root = repository.getRoot();
			rebuildStage = "traversal";
			index(root);
			if (rebuildFailed) {
				throw new IllegalStateException("Fragment index rebuild was incomplete; see logged fragment errors");
			}
			rebuildStage = "finalizeRebuild";
			if (diagnostics) {
				logger.info("Fragment index traversal complete entries={} elapsedMs={}", rebuildEntries, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
			}
			backend.finalizeRebuild();
			if (diagnostics) {
				logger.info("Fragment index rebuild complete entries={} elapsedMs={}", rebuildEntries, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
			}
		}
		catch (RuntimeException | Error e) {
			logger.error("Fragment index rebuild failed stage={} id={} entries={} elapsedMs={}", rebuildStage, rebuildId, rebuildEntries, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started), e);
			baseline.clear();
			try {
				backend.abortRebuild();
			}
			catch (RuntimeException | Error cleanup) {
				e.addSuppressed(cleanup);
			}
			throw e;
		}
		finally {
			rebuilding = false;
		}
	}

	public synchronized void handle(RepositoryEvent event) {
		if (event.getState() != RepositoryState.RELOAD) {
			return;
		}
		if (!event.isDone()) {
			reloadDepth++;
		}
		else {
			if (reloadDepth > 0) {
				reloadDepth--;
			}
			if (reloadDepth == 0) {
				pending.addAll(retryPending);
				retryPending.clear();
				reconcilePending();
			}
		}
	}

	public synchronized void handle(NodeEvent event) {
		if (!event.isDone()) {
			return;
		}
		if (event.getState() == NodeEvent.State.DELETE || event.getState() == NodeEvent.State.UNLOAD || event.getState() == NodeEvent.State.LOAD || event.getState() == NodeEvent.State.RELOAD || event.getState() == NodeEvent.State.SAVE || event.getState() == NodeEvent.State.CREATE) {
			if (reloadDepth > 0 || reconciling) {
				pending.add(event.getId());
				return;
			}
			if (event.getState() == NodeEvent.State.DELETE || event.getState() == NodeEvent.State.UNLOAD) {
				backend.delete(event.getId());
				baseline.remove(event.getId());
				retryPending.remove(event.getId());
			}
			else {
				pending.add(event.getId());
				retryPending.remove(event.getId());
				reconcilePending();
			}
		}
	}

	private void reconcilePending() {
		if (reconciling || pending.isEmpty()) {
			return;
		}
		long started = System.nanoTime();
		int affected = 0, processed = 0, deleted = 0;
		Set<String> visited = new LinkedHashSet<String>();
		Set<String> retried = new LinkedHashSet<String>();
		Set<String> failed = new LinkedHashSet<String>();
		reconciling = true;
		try {
			while (reloadDepth == 0 && !pending.isEmpty()) {
				Set<String> batch = new LinkedHashSet<String>(pending);
				pending.clear();
				for (String id : batch) {
					if (reloadDepth > 0) {
						pending.add(id);
						continue;
					}
					if (!visited.add(id)) {
						if (!retried.add(id)) {
							pending.add(id);
							continue;
						}
					}
					else {
						affected++;
					}
					try {
						Node node = repository.getNode(id);
						if (node == null) {
							backend.delete(id);
							baseline.remove(id);
							retryPending.remove(id);
							deleted++;
						}
						else {
							rebuildFailed = false;
							index(id, node);
							if (rebuildFailed) {
								failed.add(id);
							}
							else {
								retryPending.remove(id);
								processed++;
							}
						}
					}
					catch (RuntimeException | Error e) {
						logger.error("Could not reconcile fragment index for artifact: " + id, e);
						failed.add(id);
					}
				}
				if (retried.containsAll(pending) && visited.containsAll(pending)) {
					break;
				}
			}
			retryPending.addAll(failed);
		}
		finally {
			reconciling = false;
			long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
			if (elapsed >= Long.getLong("reload.slow.phase.threshold", 5000L).longValue()) {
				logger.warn("Slow fragment index batch affected={} processed={} deleted={} elapsedMs={}", affected, processed, deleted, elapsed);
			}
		}
	}

	public synchronized void refresh(String artifactId) {
		Node node = repository.getNode(artifactId);
		if (node == null) {
			backend.delete(artifactId);
			baseline.remove(artifactId);
		}
		else {
			index(artifactId, node);
		}
	}

	public FragmentSearch get(String artifactId, String path) {
		return backend.get(artifactId, path);
	}

	public List<FragmentSearch> get(List<String> artifactIds, List<String> paths) {
		return backend.get(artifactIds, paths);
	}

	public List<FragmentSearch> list(List<String> globs, List<String> namespaces, List<String> artifactTypes, List<String> artifactCategories) {
		return backend.list(globs, namespaces, artifactTypes, artifactCategories);
	}

	public List<FragmentSearch> search(String pattern, List<String> globs, List<String> namespaces, List<String> artifactTypes, List<String> artifactCategories, boolean caseSensitive, int before, int after, int limit) {
		return backend.search(pattern, globs, namespaces, artifactTypes, artifactCategories, caseSensitive, before, after, limit);
	}

	private void index(Entry entry) {
		if (entry == null) {
			return;
		}
		rebuildEntries++;
		if (rebuilding && Boolean.getBoolean("fragments.startup.diagnostics")) {
			logger.info("Fragment index visiting id={} entries={}", entry.getId(), rebuildEntries);
		}
		if (entry.isNode()) {
			rebuildId = entry.getId();
			rebuildStage = "getNode";
			index(entry.getId(), entry.getNode());
		}
		rebuildStage = "iterateChildren";
		for (Entry child : entry) {
			index(child);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void index(String artifactId, Node node) {
		Artifact artifact;
		try {
			rebuildStage = "getArtifact";
			if (rebuilding && Boolean.getBoolean("fragments.startup.diagnostics")) {
				logger.info("Fragment index loading artifact id={}", artifactId);
			}
			artifact = node.getArtifact();
		}
		catch (Exception | StackOverflowError e) {
			logger.error("Could not load artifact for fragment indexing: " + artifactId + " with artifact manager: " + node.getArtifactManager(), e);
			rebuildFailed = true;
			return;
		}
		ArtifactFragmentManager manager;
		try {
			rebuildStage = "resolveManager";
			manager = EAIRepositoryUtils.getArtifactFragmentManager(artifact);
		}
		catch (Exception | StackOverflowError e) {
			logger.error("Could not resolve fragment manager for artifact: " + artifactId + " with artifact class: " + artifact.getClass().getName(), e);
			rebuildFailed = true;
			return;
		}
		if (manager == null) {
			backend.delete(artifactId);
			baseline.remove(artifactId);
			return;
		}
		List<be.nabu.eai.repository.api.ArtifactFragmentManager.ArtifactFragment> fragments;
		try {
			rebuildStage = "listFragments";
			if (rebuilding && Boolean.getBoolean("fragments.startup.diagnostics")) {
				logger.info("Fragment index listing fragments id={} manager={}", artifactId, manager.getClass().getName());
			}
			fragments = new ArrayList<be.nabu.eai.repository.api.ArtifactFragmentManager.ArtifactFragment>(manager.listFragments(artifact));
		}
		catch (Exception | StackOverflowError e) {
			logger.error("Could not list fragments for artifact: " + artifactId + " with fragment manager: " + manager.getClass().getName(), e);
			rebuildFailed = true;
			return;
		}
		try {
			Map<String, Snapshot> previous = baseline.get(artifactId);
			Map<String, Snapshot> next = new LinkedHashMap<String, Snapshot>();
			List<ArtifactFragment> changed = new ArrayList<ArtifactFragment>();
			List<String> paths = new ArrayList<String>();
			String artifactType = manager.getArtifactType();
			String artifactCategory = manager.getArtifactCategory();
			rebuildStage = "readFragments";
			if (rebuilding && Boolean.getBoolean("fragments.startup.diagnostics")) {
				logger.info("Fragment index reading fragments id={} count={}", artifactId, fragments.size());
			}
			for (ArtifactFragment fragment : fragments) {
				String path = fragment.getPath();
				paths.add(path);
				String revision = fragment instanceof RevisionedArtifactFragment ? ((RevisionedArtifactFragment) fragment).getRevision() : null;
				Object descriptor = descriptor(artifactType, artifactCategory, fragment);
				Snapshot old = previous == null ? null : previous.get(path);
				String content = null;
				if (revision == null || old == null || !Objects.equals(revision, old.revision) || !descriptor.equals(old.descriptor)) {
					content = fragment.getContent();
				}
				if (old != null && descriptor.equals(old.descriptor) && (revision != null && revision.equals(old.revision) || revision == null && Objects.equals(content, old.content))) {
					next.put(path, old);
				}
				else {
					if (content == null) {
						content = fragment.getContent();
					}
					next.put(path, new Snapshot(revision, descriptor, content));
					changed.add(new CachedFragment(fragment, content));
				}
			}
			for (ArtifactFragment fragment : fragments) {
				Snapshot snapshot = next.get(fragment.getPath());
				if (!snapshot.descriptor.equals(descriptor(artifactType, artifactCategory, fragment)) || fragment instanceof RevisionedArtifactFragment && !Objects.equals(snapshot.revision, ((RevisionedArtifactFragment) fragment).getRevision())) {
					logger.warn("Fragment changed during indexing: {} / {}", artifactId, fragment.getPath());
					rebuildFailed = true;
					return;
				}
			}
			if (previous == null || !changed.isEmpty() || !previous.keySet().equals(next.keySet())) {
				rebuildStage = "backendIndex";
				if (rebuilding && Boolean.getBoolean("fragments.startup.diagnostics")) {
					logger.info("Fragment index writing id={} changed={} total={}", artifactId, changed.size(), paths.size());
				}
				backend.index(artifactId, artifactType, artifactCategory, node.getVersion(), changed, paths);
			}
			for (ArtifactFragment fragment : fragments) {
				Snapshot snapshot = next.get(fragment.getPath());
				if (!snapshot.descriptor.equals(descriptor(artifactType, artifactCategory, fragment)) || fragment instanceof RevisionedArtifactFragment && !Objects.equals(snapshot.revision, ((RevisionedArtifactFragment) fragment).getRevision())) {
					logger.warn("Fragment changed while storing index: {} / {}", artifactId, fragment.getPath());
					rebuildFailed = true;
					baseline.remove(artifactId);
					return;
				}
			}
			baseline.put(artifactId, next);
		}
		catch (Exception | StackOverflowError e) {
			logger.error("Could not store fragments for artifact: " + artifactId + " with fragment manager: " + manager.getClass().getName(), e);
			rebuildFailed = true;
			baseline.remove(artifactId);
		}
	}

	private Object descriptor(String artifactType, String artifactCategory, ArtifactFragment fragment) {
		return java.util.Arrays.asList(artifactType, artifactCategory, fragment.getFragmentType(), fragment.getContentType(), fragment.isEditable(), fragment.isRemovable(), fragment.getProperties() == null ? null : new LinkedHashMap<String, String>(fragment.getProperties()));
	}

	private static class Snapshot {
		private final String revision;
		private final Object descriptor;
		private final String content;
		private Snapshot(String revision, Object descriptor, String content) {
			this.revision = revision;
			this.descriptor = descriptor;
			this.content = content;
		}
	}

	private static class CachedFragment implements ArtifactFragment {
		private final String path, content, contentType, artifactId, fragmentType;
		private final Map<String, String> properties;
		private final boolean editable, removable;
		private CachedFragment(ArtifactFragment source, String content) {
			this.path = source.getPath();
			this.content = content;
			this.contentType = source.getContentType();
			this.artifactId = source.getArtifactId();
			this.fragmentType = source.getFragmentType();
			this.properties = source.getProperties() == null ? null : new LinkedHashMap<String, String>(source.getProperties());
			this.editable = source.isEditable();
			this.removable = source.isRemovable();
		}
		public String getPath() { return path; }
		public String getContent() { return content; }
		public String getContentType() { return contentType; }
		public String getArtifactId() { return artifactId; }
		public String getFragmentType() { return fragmentType; }
		public Map<String, String> getProperties() { return properties; }
		public boolean isEditable() { return editable; }
		public boolean isRemovable() { return removable; }
	}
}
