package be.nabu.eai.server.fragments;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import be.nabu.eai.repository.api.ArtifactFragmentManager.ArtifactFragment;

public class FragmentSearch implements ArtifactFragment {

	private String artifactId;
	private String path;
	private String artifactType;
	private String artifactCategory;
	private String fragmentType;
	private String content;
	private String contentType;
	private Map<String, String> properties;
	private List<String> matches;
	private boolean editable;
	private boolean removable;

	public FragmentSearch(ArtifactFragment fragment, String artifactType, String artifactCategory, List<String> matches) {
		this(fragment.getArtifactId(), fragment.getPath(), artifactType, artifactCategory, fragment.getFragmentType(), fragment.getContent(), fragment.getContentType(), fragment.getProperties(), matches, fragment.isEditable(), fragment.isRemovable());
	}

	public FragmentSearch(String artifactId, String path, String artifactType, String artifactCategory, String fragmentType, String content, String contentType, Map<String, String> properties, List<String> matches, boolean editable, boolean removable) {
		this.artifactId = artifactId;
		this.path = path;
		this.artifactType = artifactType;
		this.artifactCategory = artifactCategory;
		this.fragmentType = fragmentType;
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
