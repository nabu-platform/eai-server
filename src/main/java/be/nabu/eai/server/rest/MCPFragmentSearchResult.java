package be.nabu.eai.server.rest;

import java.util.List;
import java.util.Map;

public class MCPFragmentSearchResult {

	private String artifactId;
	private String path;
	private String artifactType;
	private String artifactCategory;
	private String fragmentType;
	private String contentType;
	private Map<String, String> properties;
	private boolean editable;
	private boolean removable;
	private List<String> matches;

	public MCPFragmentSearchResult(String artifactId, String path, String artifactType, String artifactCategory, String fragmentType, String contentType, Map<String, String> properties, boolean editable, boolean removable, List<String> matches) {
		this.artifactId = artifactId;
		this.path = path;
		this.artifactType = artifactType;
		this.artifactCategory = artifactCategory;
		this.fragmentType = fragmentType;
		this.contentType = contentType;
		this.properties = properties;
		this.editable = editable;
		this.removable = removable;
		this.matches = matches;
	}

	public String getArtifactId() {
		return artifactId;
	}

	public String getPath() {
		return path;
	}

	public String getArtifactType() {
		return artifactType;
	}

	public String getArtifactCategory() {
		return artifactCategory;
	}

	public String getFragmentType() {
		return fragmentType;
	}

	public String getContentType() {
		return contentType;
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	public boolean isEditable() {
		return editable;
	}

	public boolean isRemovable() {
		return removable;
	}

	public List<String> getMatches() {
		return matches;
	}
}
