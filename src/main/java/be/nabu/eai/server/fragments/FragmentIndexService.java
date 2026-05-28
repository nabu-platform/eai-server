package be.nabu.eai.server.fragments;

import java.util.ArrayList;
import java.util.List;

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

	public FragmentIndexService(Repository repository, FragmentIndexBackend backend) {
		this.repository = repository;
		this.backend = backend;
	}

	public void initialize() {
		backend.initialize();
	}

	public void rebuild() {
		backend.beginRebuild();
		try {
			index(repository.getRoot());
		}
		finally {
			backend.finalizeRebuild();
		}
	}

	public void handle(NodeEvent event) {
		if (!event.isDone()) {
			return;
		}
		if (event.getState() == NodeEvent.State.DELETE || event.getState() == NodeEvent.State.UNLOAD) {
			backend.delete(event.getId());
			return;
		}
		if (event.getState() == NodeEvent.State.LOAD || event.getState() == NodeEvent.State.RELOAD || event.getState() == NodeEvent.State.SAVE || event.getState() == NodeEvent.State.CREATE) {
			Node node = repository.getNode(event.getId());
			if (node == null) {
				backend.delete(event.getId());
			}
			else {
				index(event.getId(), node);
			}
		}
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
		if (entry.isNode()) {
			index(entry.getId(), entry.getNode());
		}
		for (Entry child : entry) {
			index(child);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void index(String artifactId, Node node) {
		try {
			Artifact artifact = node.getArtifact();
			ArtifactFragmentManager manager = EAIRepositoryUtils.getArtifactFragmentManager(artifact);
			if (manager == null) {
				backend.delete(artifactId);
				return;
			}
			List<be.nabu.eai.repository.api.ArtifactFragmentManager.ArtifactFragment> fragments = new ArrayList<be.nabu.eai.repository.api.ArtifactFragmentManager.ArtifactFragment>(manager.listFragments(artifact));
			backend.index(artifactId, manager.getArtifactType(), manager.getArtifactCategory(), node.getVersion(), fragments);
		}
		catch (Exception e) {
			logger.error("Could not index fragments for artifact: " + artifactId, e);
		}
	}
}
