package be.nabu.eai.server.fragments;

import java.util.List;

import be.nabu.eai.repository.api.ArtifactFragmentManager.ArtifactFragment;

public interface FragmentIndexBackend {

	public void initialize();
	public void beginRebuild();
	public void index(String artifactId, long version, List<ArtifactFragment> fragments);
	public void finalizeRebuild();
	public void delete(String artifactId);
	public List<FragmentSearch> search(String pattern, List<String> globs, List<String> namespaces, int before, int after, int limit);
}
