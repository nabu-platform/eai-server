package be.nabu.eai.server.documentation;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DocumentationSearch {

	private String namespace;
	private String path;
	private String content;
	private String contentType;
	private Map<String, String> properties;
	private List<String> matches;
	private boolean editable;
	private boolean removable;
	private Long lastModified;

	public DocumentationSearch(String namespace, String path, String content, String contentType, Map<String, String> properties, List<String> matches) {
		this(namespace, path, content, contentType, properties, matches, false, false, null);
	}

	public DocumentationSearch(String namespace, String path, String content, String contentType, Map<String, String> properties, List<String> matches, boolean editable, boolean removable, Long lastModified) {
		this.namespace = namespace;
		this.path = path;
		this.content = content;
		this.contentType = contentType;
		this.properties = properties == null ? Collections.<String, String>emptyMap() : properties;
		this.matches = matches == null ? Collections.<String>emptyList() : matches;
		this.editable = editable;
		this.removable = removable;
		this.lastModified = lastModified;
	}

	public String getNamespace() {
		return namespace;
	}

	public String getPath() {
		return path;
	}

	public String getContent() {
		return content;
	}

	public String getContentType() {
		return contentType;
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	public List<String> getMatches() {
		return matches;
	}

	public boolean isEditable() {
		return editable;
	}

	public boolean isRemovable() {
		return removable;
	}

	public Long getLastModified() {
		return lastModified;
	}
}
