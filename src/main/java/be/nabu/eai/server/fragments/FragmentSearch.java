package be.nabu.eai.server.fragments;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import be.nabu.eai.repository.api.ArtifactFragmentManager.ArtifactFragment;

public class FragmentSearch implements ArtifactFragment {

	private String artifactId;
	private String path;
	private String artifactType;
	private String content;
	private String contentType;
	private Map<String, String> properties;
	private List<String> matches;
	private boolean editable;
	private boolean removable;

	public FragmentSearch(ArtifactFragment fragment, List<String> matches) {
		this(fragment.getArtifactId(), fragment.getPath(), fragment.getArtifactType(), fragment.getContent(), fragment.getContentType(), fragment.getProperties(), matches, fragment.isEditable(), fragment.isRemovable());
	}

	public FragmentSearch(String artifactId, String path, String artifactType, String content, String contentType, Map<String, String> properties, List<String> matches, boolean editable, boolean removable) {
		this.artifactId = artifactId;
		this.path = path;
		this.artifactType = artifactType;
		this.content = content;
		this.contentType = contentType;
		this.properties = properties == null ? Collections.<String, String>emptyMap() : properties;
		this.matches = matches;
		this.editable = editable;
		this.removable = removable;
	}

	@Override
	public boolean isEditable() {
		return editable;
	}

	@Override
	public boolean isRemovable() {
		return removable;
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

	@Override
	public String getArtifactType() {
		return artifactType;
	}

	@Override
	public Map<String, String> getProperties() {
		return properties;
	}

	public List<String> getMatches() {
		return matches;
	}
}
