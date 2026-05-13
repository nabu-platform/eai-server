package be.nabu.eai.server.rest;

import java.io.Serializable;
import java.util.List;

public class MCPConfiguration implements Serializable {

	private static final long serialVersionUID = 1L;

	private List<String> namespace;

	public List<String> getNamespace() {
		return namespace;
	}

	public void setNamespace(List<String> namespace) {
		this.namespace = namespace;
	}
}
