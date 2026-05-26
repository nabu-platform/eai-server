package be.nabu.eai.server.fragments;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import be.nabu.eai.repository.api.ArtifactFragmentManager.ArtifactFragment;

public class FragmentSearchResult implements ArtifactFragment {

	private String artifactId;
	private String artifactType;
	private String artifactCategory;
	private String fragmentType;
	private String path;
	private String content;
	private String contentType;
	private Map<String, String> properties;
	private List<String> matches;

	public FragmentSearchResult(ArtifactFragment fragment, String artifactType, String artifactCategory, List<String> matches) {
		this.artifactId = fragment.getArtifactId();
		this.artifactType = artifactType;
		this.artifactCategory = artifactCategory;
		this.fragmentType = fragment.getFragmentType();
		this.path = fragment.getPath();
		this.content = fragment.getContent();
		this.contentType = fragment.getContentType();
		this.properties = fragment.getProperties() == null ? Collections.<String, String>emptyMap() : fragment.getProperties();
		this.matches = matches;
	}

	@Override
	public boolean isEditable() {
		return false;
	}

	@Override
	public boolean isRemovable() {
		return false;
	}

	@Override
	public String getPath() {
		return path;
	}

	@Override
	public String getContent() {
		return content;
	}

	@Override
	public String getContentType() {
		return contentType;
	}

	@Override
	public String getArtifactId() {
		return artifactId;
	}

	public String getArtifactType() {
		return artifactType;
	}

	public String getArtifactCategory() {
		return artifactCategory;
	}

	@Override
	public String getFragmentType() {
		return fragmentType;
	}

	@Override
	public Map<String, String> getProperties() {
		return properties;
	}

	@Override
	public Long getLastModified() {
		return null;
	}

	public List<String> getMatches() {
		return matches;
	}
}
