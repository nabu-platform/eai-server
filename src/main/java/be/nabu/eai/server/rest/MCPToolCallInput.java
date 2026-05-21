package be.nabu.eai.server.rest;

import java.util.List;

public class MCPToolCallInput {

	private String pattern;
	private List<String> glob;
	private Boolean caseSensitive;
	private Integer beforeContext;
	private Integer afterContext;
	private Integer context;
	private Integer limit;
	private Integer offset;
	private List<String> namespace;
	private List<String> artifactType;
	private List<String> artifactCategory;

	public String getPattern() {
		return pattern;
	}

	public void setPattern(String pattern) {
		this.pattern = pattern;
	}

	public List<String> getGlob() {
		return glob;
	}

	public void setGlob(List<String> glob) {
		this.glob = glob;
	}

	public Boolean getCaseSensitive() {
		return caseSensitive;
	}

	public void setCaseSensitive(Boolean caseSensitive) {
		this.caseSensitive = caseSensitive;
	}

	public Integer getBeforeContext() {
		return beforeContext;
	}

	public void setBeforeContext(Integer beforeContext) {
		this.beforeContext = beforeContext;
	}

	public Integer getAfterContext() {
		return afterContext;
	}

	public void setAfterContext(Integer afterContext) {
		this.afterContext = afterContext;
	}

	public Integer getContext() {
		return context;
	}

	public void setContext(Integer context) {
		this.context = context;
	}

	public Integer getLimit() {
		return limit;
	}

	public void setLimit(Integer limit) {
		this.limit = limit;
	}

	public Integer getOffset() {
		return offset;
	}

	public void setOffset(Integer offset) {
		this.offset = offset;
	}

	public List<String> getNamespace() {
		return namespace;
	}

	public void setNamespace(List<String> namespace) {
		this.namespace = namespace;
	}

	public List<String> getArtifactType() {
		return artifactType;
	}

	public void setArtifactType(List<String> artifactType) {
		this.artifactType = artifactType;
	}

	public List<String> getArtifactCategory() {
		return artifactCategory;
	}

	public void setArtifactCategory(List<String> artifactCategory) {
		this.artifactCategory = artifactCategory;
	}
}
