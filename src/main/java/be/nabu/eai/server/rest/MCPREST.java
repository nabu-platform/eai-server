package be.nabu.eai.server.rest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import be.nabu.eai.repository.EAIRepositoryUtils;
import be.nabu.eai.repository.api.ArtifactFragmentManager;
import be.nabu.eai.repository.api.ReviewableArtifactFragmentManager;
import be.nabu.eai.repository.api.ReviewableArtifactFragmentManager.ReviewResource;
import be.nabu.eai.repository.api.ArtifactFragmentManager.ArtifactFragment;
import be.nabu.eai.repository.api.Node;
import be.nabu.libs.artifacts.api.Artifact;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import be.nabu.eai.server.Server;
import be.nabu.eai.server.fragments.FragmentIndexService;
import be.nabu.eai.server.fragments.FragmentSearch;
import be.nabu.libs.cluster.api.ClusterMap;
import be.nabu.libs.http.HTTPException;
import be.nabu.libs.http.api.HTTPRequest;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.binding.api.Window;
import be.nabu.libs.types.binding.json.JSONBinding;
import be.nabu.libs.types.java.BeanInstance;
import be.nabu.libs.types.map.MapContent;
import be.nabu.libs.types.map.MapContentWrapper;
import be.nabu.libs.types.map.MapTypeGenerator;
import be.nabu.libs.validator.api.Validation;
import be.nabu.libs.validator.api.ValidationMessage.Severity;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.mime.api.Header;
import be.nabu.utils.mime.impl.MimeHeader;
import be.nabu.utils.mime.impl.PlainMimeContentPart;

@Path("/mcp")
public class MCPREST {

	private static class ToolResult {
		private final Map<String, Object> structuredContent;
		private final List<Map<String, String>> content;
		private final Map<String, Object> meta;

		private ToolResult(Map<String, Object> structuredContent, List<Map<String, String>> content, Map<String, Object> meta) {
			this.structuredContent = structuredContent;
			this.content = content;
			this.meta = meta;
		}
	}

	private static class EditArtifactResult {
		private final Map<String, Object> structuredContent;
		private final String resourceUri;

		private EditArtifactResult(Map<String, Object> structuredContent, String resourceUri) {
			this.structuredContent = structuredContent;
			this.resourceUri = resourceUri;
		}
	}

	private static final String MCP_VERSION = "2025-03-26";
	private static final String SEARCH_TOOL_NAME = "search_nabu_artifact_fragments";
	private static final String FIND_TOOL_NAME = "find_nabu_artifact_fragment";
	private static final String READ_TOOL_NAME = "read_nabu_artifact_fragment";
	private static final String EDIT_TOOL_NAME = "edit_nabu_artifact_fragment";
	private static final String WRITE_TOOL_NAME = "write_nabu_artifact_fragment";
	private static final String SKILLS_TOOL_NAME = "get_nabu_artifact_fragment_skills";
	private static final String MCP_SESSION_ID = "MCP-Session-Id";
	private static final String SESSION_MAP = "mcp.rest.sessions";
	private static final String REVIEW_RESOURCE_MAP = "mcp.rest.review.resources";
	private static final String REVIEW_RESOURCE_URI = "ui://nabu/review/diff.html";
	private static final int MAX_RESULT_BYTES = 32000;
	private static final int SUMMARY_TOP = 20;
	private static final long SESSION_TIMEOUT = 24L * 60L * 60L * 1000L;

	@Context
	private Server server;

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public PlainMimeContentPart handle(InputStream content, HTTPRequest request, Header...headers) throws IOException, ParseException {
		Map<String, Object> rpc = parse(content);
		Object id = rpc.get("id");
		String method = string(rpc.get("method"));
		Map<String, Object> response = new LinkedHashMap<String, Object>();
		response.put("jsonrpc", "2.0");
		response.put("id", id);
		if (method == null) {
			response.put("error", error(-32600, "Invalid request"));
			return json(response, null);
		}
		if ("initialize".equals(method)) {
			String sessionId = UUID.randomUUID().toString();
			MCPConfiguration configuration = new MCPConfiguration();
			applyInitializeConfiguration(configuration, map(rpc.get("params")));
			storeSession(sessionId, configuration);
			Map<String, Object> result = new LinkedHashMap<String, Object>();
			result.put("protocolVersion", MCP_VERSION);
			Map<String, Object> capabilities = new LinkedHashMap<String, Object>();
			Map<String, Object> experimental = new LinkedHashMap<String, Object>();
			experimental.put("policy", true);
			capabilities.put("experimental", experimental);
			Map<String, Object> resources = new LinkedHashMap<String, Object>();
			resources.put("list", true);
			resources.put("read", true);
			capabilities.put("resources", resources);
			result.put("capabilities", capabilities);
			Map<String, Object> serverInfo = new LinkedHashMap<String, Object>();
			serverInfo.put("name", "nabu-mcp");
			serverInfo.put("version", "1.0");
			result.put("serverInfo", serverInfo);
			result.put("configSchema", configSchema());
			response.put("result", result);
			return json(response, sessionId);
		}
		if ("resources/list".equals(method)) {
			resolveSessionConfiguration(request, false);
			Map<String, Object> result = new LinkedHashMap<String, Object>();
			result.put("resources", listResources());
			response.put("result", result);
			return json(response, null);
		}
		if ("resources/read".equals(method)) {
			resolveSessionConfiguration(request, false);
			Map<String, Object> params = map(rpc.get("params"));
			Map<String, Object> result = new LinkedHashMap<String, Object>();
			result.put("contents", readResource(params == null ? null : string(params.get("uri"))));
			response.put("result", result);
			return json(response, null);
		}
		if ("tools/list".equals(method)) {
			resolveSessionConfiguration(request, false);
			Map<String, Object> result = new LinkedHashMap<String, Object>();
			List<Map<String, Object>> tools = new ArrayList<Map<String, Object>>();
			Map<String, Object> searchTool = new LinkedHashMap<String, Object>();
			searchTool.put("name", SEARCH_TOOL_NAME);
			searchTool.put("title", "Search nabu artifacts");
			searchTool.put("description", "Search indexed artifact fragments ripgrep style. Namespace filters artifacts by id prefix, while glob only filters fragment paths.");
			Map<String, Object> searchInputSchema = new LinkedHashMap<String, Object>();
			searchInputSchema.put("type", "object");
			Map<String, Object> searchProperties = new LinkedHashMap<String, Object>();
			searchProperties.put("pattern", schema("string"));
			Map<String, Object> glob = schema("array");
			glob.put("items", schema("string"));
			glob.put("description", "Optional ripgrep-style glob filters applied only to fragment paths, not artifact ids.");
			searchProperties.put("glob", glob);
			Map<String, Object> namespace = schema("array");
			namespace.put("items", schema("string"));
			namespace.put("description", "Optional artifact namespace filters. Matches the exact namespace and all descendant artifact ids. Configured and policy namespaces are applied first; this argument can only narrow further.");
			searchProperties.put("namespace", namespace);
			searchProperties.put("case_sensitive", schema("string"));
			searchProperties.put("before_context", schema("integer"));
			searchProperties.put("after_context", schema("integer"));
			searchProperties.put("context", schema("integer"));
			searchInputSchema.put("properties", searchProperties);
			searchInputSchema.put("required", Arrays.asList("pattern"));
			searchTool.put("inputSchema", searchInputSchema);
			searchTool.put("outputSchema", searchOutputSchema());
			tools.add(searchTool);
			Map<String, Object> findTool = new LinkedHashMap<String, Object>();
			findTool.put("name", FIND_TOOL_NAME);
			findTool.put("title", "Find nabu artifact fragments");
			findTool.put("description", "Find indexed nabu artifact fragments using path and artifact filters.");
			Map<String, Object> findInputSchema = new LinkedHashMap<String, Object>();
			findInputSchema.put("type", "object");
			Map<String, Object> findProperties = new LinkedHashMap<String, Object>();
			findProperties.put("pattern", propertySchema("string", "Pattern to match against fragment paths or artifact ids."));
			findProperties.put("artifact_id", propertySchema("string", "Optional artifact id filter."));
			findProperties.put("path", propertySchema("string", "Optional exact fragment path filter."));
			findProperties.put("glob", propertySchema("boolean", "If true, interpret pattern as a glob instead of a regex."));
			findProperties.put("limit", propertySchema("integer", "Maximum number of results to return (>0)."));
			findProperties.put("offset", propertySchema("integer", "Number of matching results to skip before returning results."));
			findProperties.put("case_sensitive", propertySchema("string", "Case sensitivity: auto|true|false."));
			findInputSchema.put("properties", findProperties);
			findTool.put("inputSchema", findInputSchema);
			findTool.put("outputSchema", findOutputSchema());
			tools.add(findTool);
			Map<String, Object> readTool = new LinkedHashMap<String, Object>();
			readTool.put("name", READ_TOOL_NAME);
			readTool.put("title", "Read nabu artifact fragment");
			readTool.put("description", "Read lines from an indexed nabu artifact fragment.");
			Map<String, Object> readInputSchema = new LinkedHashMap<String, Object>();
			readInputSchema.put("type", "object");
			Map<String, Object> readProperties = new LinkedHashMap<String, Object>();
			readProperties.put("artifact_id", propertySchema("string", "Artifact id containing the fragment."));
			readProperties.put("path", propertySchema("string", "Path to the fragment inside the artifact."));
			readProperties.put("start_line", propertySchema("integer", "1-based line number to start reading from. Default: 1."));
			readProperties.put("limit", propertySchema("integer", "Maximum number of lines to return (>0). Default: 200."));
			readInputSchema.put("properties", readProperties);
			readInputSchema.put("required", Arrays.asList("artifact_id", "path"));
			readTool.put("inputSchema", readInputSchema);
			readTool.put("outputSchema", readOutputSchema());
			tools.add(readTool);
			Map<String, Object> editTool = new LinkedHashMap<String, Object>();
			editTool.put("name", EDIT_TOOL_NAME);
			editTool.put("title", "Edit nabu artifact fragment");
			editTool.put("description", "Replace exact matches in an editable artifact fragment.");
			Map<String, Object> annotations = new LinkedHashMap<String, Object>();
			annotations.put("preview", true);
			editTool.put("annotations", annotations);
			Map<String, Object> editInputSchema = new LinkedHashMap<String, Object>();
			editInputSchema.put("type", "object");
			Map<String, Object> editProperties = new LinkedHashMap<String, Object>();
			editProperties.put("edits", editSchema());
			editInputSchema.put("properties", editProperties);
			editInputSchema.put("required", Arrays.asList("edits"));
			editTool.put("inputSchema", editInputSchema);
			editTool.put("outputSchema", editOutputSchema());
			tools.add(editTool);
			Map<String, Object> writeTool = new LinkedHashMap<String, Object>();
			writeTool.put("name", WRITE_TOOL_NAME);
			writeTool.put("title", "Write nabu artifact fragment");
			writeTool.put("description", "Use this tool to overwrite, append, or prepend a whole editable artifact fragment.");
			writeTool.put("annotations", annotations);
			Map<String, Object> writeInputSchema = new LinkedHashMap<String, Object>();
			writeInputSchema.put("type", "object");
			Map<String, Object> writeProperties = new LinkedHashMap<String, Object>();
			writeProperties.put("artifact_id", propertySchema("string", "Artifact id containing the fragment."));
			writeProperties.put("path", propertySchema("string", "Path to the fragment inside the artifact."));
			writeProperties.put("content", propertySchema("string", "New fragment content to write or preview."));
			Map<String, Object> mode = propertySchema("string", "Write mode. Default: overwrite.");
			mode.put("enum", Arrays.asList("overwrite", "append", "prepend"));
			writeProperties.put("mode", mode);
			writeInputSchema.put("properties", writeProperties);
			writeInputSchema.put("required", Arrays.asList("artifact_id", "path", "content"));
			writeTool.put("inputSchema", writeInputSchema);
			writeTool.put("outputSchema", writeOutputSchema());
			tools.add(writeTool);
			Map<String, Object> skillsTool = new LinkedHashMap<String, Object>();
			skillsTool.put("name", SKILLS_TOOL_NAME);
			skillsTool.put("title", "Get nabu artifact fragment skills");
			skillsTool.put("description", "Fetch artifact and fragment-specific editing skills. Use this whenever you want to edit a nabu artifact fragment type you do not know yet. You can request multiple artifact/fragment combinations in one call.");
			Map<String, Object> skillsInputSchema = new LinkedHashMap<String, Object>();
			skillsInputSchema.put("type", "object");
			Map<String, Object> skillsProperties = new LinkedHashMap<String, Object>();
			Map<String, Object> requests = schema("array");
			Map<String, Object> requestItem = new LinkedHashMap<String, Object>();
			requestItem.put("type", "object");
			Map<String, Object> requestProperties = new LinkedHashMap<String, Object>();
			requestProperties.put("artifact_type", propertySchema("string", "Logical artifact type, for example structure or blox."));
			Map<String, Object> fragmentTypes = schema("array");
			fragmentTypes.put("items", schema("string"));
			fragmentTypes.put("description", "Optional fragment types to filter the returned guidance, for example metadata, structure, pipeline or service.");
			requestProperties.put("fragment_types", fragmentTypes);
			requestItem.put("properties", requestProperties);
			requestItem.put("required", Arrays.asList("artifact_type"));
			requests.put("items", requestItem);
			skillsProperties.put("requests", requests);
			skillsInputSchema.put("properties", skillsProperties);
			skillsInputSchema.put("required", Arrays.asList("requests"));
			skillsTool.put("inputSchema", skillsInputSchema);
			tools.add(skillsTool);
			result.put("tools", tools);
			response.put("result", result);
			return json(response, null);
		}
		if ("tools/call".equals(method)) {
			MCPConfiguration configuration = resolveSessionConfiguration(request, true);
			Map<String, Object> params = map(rpc.get("params"));
			String name = params == null ? null : string(params.get("name"));
			if (!SEARCH_TOOL_NAME.equals(name) && !FIND_TOOL_NAME.equals(name) && !READ_TOOL_NAME.equals(name) && !EDIT_TOOL_NAME.equals(name) && !WRITE_TOOL_NAME.equals(name) && !SKILLS_TOOL_NAME.equals(name)) {
				response.put("error", error(-32602, "Unknown tool: " + name));
				return json(response, null);
			}
			Map<String, Object> arguments = map(params.get("arguments"));
			Map<String, Object> meta = map(params.get("_meta"));
			Map<String, Object> result = new LinkedHashMap<String, Object>();
			ToolResult toolResult;
			if (SEARCH_TOOL_NAME.equals(name)) {
				toolResult = searchToolResult(arguments, meta, configuration);
			}
			else if (FIND_TOOL_NAME.equals(name)) {
				toolResult = findToolResult(arguments, meta, configuration);
			}
			else if (READ_TOOL_NAME.equals(name)) {
				toolResult = readToolResult(arguments, meta, configuration);
			}
			else if (SKILLS_TOOL_NAME.equals(name)) {
				toolResult = skillsToolResult(arguments);
			}
			else {
				boolean preview = isPreview(meta == null ? null : meta.get("preview"));
				toolResult = EDIT_TOOL_NAME.equals(name)
					? editToolResult(arguments, meta, configuration, preview)
					: writeToolResult(arguments, meta, configuration, preview);
			}
			result.put("content", toolResult.content);
			result.put("structuredContent", toolResult.structuredContent);
			if (toolResult.meta != null && !toolResult.meta.isEmpty()) {
				result.put("_meta", toolResult.meta);
			}
			response.put("result", result);
			return json(response, null);
		}
		response.put("error", error(-32601, "Method not found: " + method));
		return json(response, null);
	}

	private ToolResult searchToolResult(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration) throws IOException, ParseException {
		MCPToolCallInput input = bind(arguments, MCPToolCallInput.class);
		List<MCPFragmentSearchResult> results = search(input, meta, configuration);
		Map<String, Object> structuredContent = optimizeResults(input.getPattern(), results);
		List<Map<String, String>> content = textContent(buildSummaryText(structuredContent));
		return new ToolResult(structuredContent, content, null);
	}

	private ToolResult skillsToolResult(Map<String, Object> arguments) {
		List<Map<String, Object>> requests = listOfMaps(arguments == null ? null : arguments.get("requests"));
		String markdown = buildSkillsMarkdown(requests);
		Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
		structuredContent.put("markdown", markdown);
		structuredContent.put("count", requests.size());
		return new ToolResult(structuredContent, textContent(markdown), null);
	}

	private String buildSkillsMarkdown(List<Map<String, Object>> requests) {
		if (requests == null || requests.isEmpty()) {
			requests = Arrays.asList(
				requestForArtifactType("structure"),
				requestForArtifactType("blox"),
				requestForArtifactType("service"),
				requestForArtifactType("complexType")
			);
		}
		StringBuilder builder = new StringBuilder();
		for (Map<String, Object> request : requests) {
			String artifactType = requiredString(request, "artifact_type", "MISSING_ARTIFACT_TYPE");
			List<String> fragmentTypes = stringList(request.get("fragment_types"));
			ArtifactFragmentManager<?> manager = findFragmentManagerByArtifactType(artifactType);
			builder.append("# Artifact: `").append(artifactType).append("`\n\n");
			if (fragmentTypes != null && !fragmentTypes.isEmpty()) {
				builder.append("Requested fragments: `").append(String.join("`, `", fragmentTypes)).append("`\n\n");
			}
			if (manager == null) {
				builder.append("No artifact fragment manager was found for this artifact type.\n\n");
				continue;
			}
			String guidelines = manager.getGuidelines(fragmentTypes);
			if (guidelines == null || guidelines.trim().isEmpty()) {
				builder.append("No guidance is available for the requested fragment types.\n\n");
			}
			else {
				builder.append(guidelines.trim()).append("\n\n");
			}
		}
		return builder.toString().trim();
	}

	@SuppressWarnings("rawtypes")
	private ArtifactFragmentManager<?> findFragmentManagerByArtifactType(String artifactType) {
		try {
			if ("structure".equals(artifactType)) {
				return (ArtifactFragmentManager) Class.forName("be.nabu.eai.module.types.structure.StructureArtifactFragmentManager", true, server.getRepository().getClassLoader()).newInstance();
			}
			if ("blox".equals(artifactType)) {
				return (ArtifactFragmentManager) Class.forName("be.nabu.eai.module.services.vm.VMServiceArtifactFragmentManager", true, server.getRepository().getClassLoader()).newInstance();
			}
			if ("service".equals(artifactType)) {
				return (ArtifactFragmentManager) Class.forName("be.nabu.eai.repository.impl.DefinedServiceArtifactFragmentManager", true, server.getRepository().getClassLoader()).newInstance();
			}
			if ("complexType".equals(artifactType) || "simpleType".equals(artifactType)) {
				return (ArtifactFragmentManager) Class.forName("be.nabu.eai.repository.impl.DefinedTypeArtifactFragmentManager", true, server.getRepository().getClassLoader()).newInstance();
			}
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
		return null;
	}

	private Map<String, Object> requestForArtifactType(String artifactType) {
		Map<String, Object> request = new LinkedHashMap<String, Object>();
		request.put("artifact_type", artifactType);
		return request;
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> listOfMaps(Object value) {
		if (!(value instanceof List)) {
			return null;
		}
		List<Map<String, Object>> maps = new ArrayList<Map<String, Object>>();
		for (Object single : (List<Object>) value) {
			Map<String, Object> map = map(single);
			if (map != null) {
				maps.add(map);
			}
		}
		return maps;
	}

	@SuppressWarnings("unchecked")
	private List<String> stringList(Object value) {
		if (!(value instanceof List)) {
			return null;
		}
		List<String> strings = new ArrayList<String>();
		for (Object single : (List<Object>) value) {
			String string = string(single);
			if (string != null && !string.trim().isEmpty()) {
				strings.add(string.trim());
			}
		}
		return strings.isEmpty() ? null : strings;
	}

	private ToolResult findToolResult(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration) {
		Map<String, Object> structuredContent = findArtifactFragments(arguments, meta, configuration);
		List<Map<String, String>> content = textContent(buildFindSummaryText(structuredContent));
		return new ToolResult(structuredContent, content, null);
	}

	private ToolResult readToolResult(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration) {
		Map<String, Object> structuredContent = readArtifact(arguments, meta, configuration);
		List<Map<String, String>> content = textContent(buildReadSummaryText(structuredContent));
		return new ToolResult(structuredContent, content, null);
	}

	private ToolResult editToolResult(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration, boolean preview) throws IOException, ParseException {
		EditArtifactResult editResult = editArtifact(arguments, meta, configuration, preview);
		List<Map<String, String>> content = textContent(buildEditSummaryText(editResult.structuredContent));
		Map<String, Object> toolMeta = buildToolMeta(editResult.resourceUri);
		return new ToolResult(editResult.structuredContent, content, toolMeta);
	}

	private ToolResult writeToolResult(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration, boolean preview) throws IOException, ParseException {
		EditArtifactResult writeResult = writeArtifact(arguments, meta, configuration, preview);
		List<Map<String, String>> content = textContent(buildWriteSummaryText(writeResult.structuredContent));
		Map<String, Object> toolMeta = buildToolMeta(writeResult.resourceUri);
		return new ToolResult(writeResult.structuredContent, content, toolMeta);
	}

	private EditArtifactResult editArtifact(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration, boolean preview) throws IOException, ParseException {
		List<String> namespaces = resolveNamespaces(configuration, null, meta);
		List<Map<String, String>> edits = extractEdits(arguments);
		List<Map<String, Object>> operations = new ArrayList<Map<String, Object>>();
		for (Map<String, String> edit : edits) {
			String artifactId = edit.get("artifact_id");
			String path = edit.get("path");
			if (!isAllowedNamespace(artifactId, namespaces)) {
				throw protocolError("INVALID_PATH", "invalid path " + artifactId + ": path outside allowed namespace");
			}
			Node currentNode = server.getRepository().getNode(artifactId);
			if (currentNode == null) {
				throw protocolError("INVALID_PATH", "invalid path " + artifactId + ": artifact not found");
			}
			Artifact currentArtifact = (Artifact) currentNode.getArtifact();
			ArtifactFragmentManager<Artifact> currentManager = EAIRepositoryUtils.getArtifactFragmentManager(currentArtifact);
			if (currentManager == null) {
				throw protocolError("INVALID_PATH", "invalid path " + artifactId + ": no fragment manager found");
			}
			ArtifactFragment currentFragment = findEditableFragment(currentManager, currentArtifact, artifactId, path);
			String currentContent = currentFragment.getContent();
			String find = edit.get("find");
			int matchCount = countMatches(currentContent, find);
			if (matchCount == 0) {
				throw protocolError("FIND_NOT_FOUND", "find text not found for " + artifactId + " / " + path);
			}
			if (matchCount > 1) {
				throw protocolError("FIND_NOT_UNIQUE", "find text not unique for " + artifactId + " / " + path);
			}
			Map<String, Object> operation = new LinkedHashMap<String, Object>();
			operation.put("artifactId", artifactId);
			operation.put("path", path);
			operation.put("artifact", currentArtifact);
			operation.put("manager", currentManager);
			operation.put("before", currentContent);
			operation.put("after", currentContent.replace(find, edit.get("replace")));
			operation.put("match_count", matchCount);
			operations.add(operation);
		}
		List<Map<String, Object>> updates = new ArrayList<Map<String, Object>>();
		int successCount = 0;
		for (Map<String, Object> operation : operations) {
			Map<String, Object> update = new LinkedHashMap<String, Object>();
			String artifactId = (String) operation.get("artifactId");
			String path = (String) operation.get("path");
			update.put("artifactId", artifactId);
			update.put("path", path);
			update.put("match_count", operation.get("match_count"));
			if (preview) {
				update.put("updated", false);
				update.put("preview", true);
				successCount++;
			}
			else {
				try {
					@SuppressWarnings("unchecked")
					ArtifactFragmentManager<Artifact> currentManager = (ArtifactFragmentManager<Artifact>) operation.get("manager");
					Artifact currentArtifact = (Artifact) operation.get("artifact");
					String before = (String) operation.get("before");
					String after = (String) operation.get("after");
					List<Validation<?>> validations = currentManager.updateFragment(currentArtifact, path, before, after);
					update.put("updated", !hasErrors(validations));
					if (validations != null && !validations.isEmpty()) {
						update.put("validations", validationMaps(validations));
					}
					if (hasErrors(validations)) {
						update.put("error", buildValidationMessage(validations));
					}
					else {
						reloadArtifactAfterMcpUpdate(artifactId);
						notifyCollaborationReload(artifactId);
						successCount++;
					}
				}
				catch (Exception e) {
					update.put("updated", false);
					update.put("error", e.getMessage() == null ? e.getClass().getName() : e.getMessage());
				}
			}
			updates.add(update);
		}
		Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
		structuredContent.put("count", updates.size());
		structuredContent.put("updated_count", successCount);
		structuredContent.put("failed_count", updates.size() - successCount);
		structuredContent.put("isError", successCount == 0);
		structuredContent.put("preview", preview);
		structuredContent.put("updates", updates);
		if (operations.size() == 1) {
			Map<String, Object> operation = operations.get(0);
			structuredContent.put("path", operation.get("path"));
			structuredContent.put("match_count", operation.get("match_count"));
			structuredContent.put("original", operation.get("before"));
			structuredContent.put("new", operation.get("after"));
			structuredContent.put("diff", buildFallbackDiff(Arrays.asList(operation)));
		}
		ensureStaticReviewResource();
		return new EditArtifactResult(structuredContent, REVIEW_RESOURCE_URI);
	}

	private Map<String, Object> findArtifactFragments(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration) {
		List<String> namespaces = resolveNamespaces(configuration, null, meta);
		String artifactId = string(arguments.get("artifact_id"));
		String path = string(arguments.get("path"));
		String pattern = string(arguments.get("pattern"));
		boolean glob = isPreview(arguments.get("glob"));
		int limit = integer(arguments.get("limit"), 200);
		int offset = integer(arguments.get("offset"), 0);
		if (limit <= 0) {
			throw protocolError("INVALID_LIMIT", "limit must be a positive integer");
		}
		if (offset < 0) {
			throw protocolError("INVALID_OFFSET", "offset must be a non-negative integer");
		}
		List<FragmentSearch> fragments = server.getFragmentIndexService() == null
			? Collections.<FragmentSearch>emptyList()
			: server.getFragmentIndexService().search(".*", null, namespaces, 0, 0, 0);
		List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
		for (FragmentSearch fragment : fragments) {
			if (artifactId != null && !artifactId.equals(fragment.getArtifactId())) {
				continue;
			}
			if (path != null && !path.equals(fragment.getPath())) {
				continue;
			}
			if (pattern != null && !pattern.trim().isEmpty() && !matchesFindPattern(pattern, glob, fragment.getArtifactId(), fragment.getPath())) {
				continue;
			}
			Map<String, Object> entry = new LinkedHashMap<String, Object>();
			entry.put("artifact_id", fragment.getArtifactId());
			entry.put("path", fragment.getPath());
			entry.put("artifact_type", fragment.getArtifactType());
			entry.put("fragment_type", fragment.getFragmentType());
			entry.put("content_type", fragment.getContentType());
			entry.put("editable", fragment.isEditable());
			entry.put("removable", fragment.isRemovable());
			entry.put("properties", fragment.getProperties());
			results.add(entry);
		}
		int total = results.size();
		int from = Math.min(offset, total);
		int to = Math.min(from + limit, total);
		List<Map<String, Object>> page = new ArrayList<Map<String, Object>>(results.subList(from, to));
		Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
		structuredContent.put("files", page);
		structuredContent.put("count", page.size());
		structuredContent.put("total", total);
		structuredContent.put("limit", limit);
		structuredContent.put("offset", offset);
		structuredContent.put("truncated", to < total);
		return structuredContent;
	}

	private Map<String, Object> readArtifact(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration) {
		List<String> namespaces = resolveNamespaces(configuration, null, meta);
		String artifactId = requiredString(arguments, "artifact_id", "MISSING_ARTIFACT_ID");
		String path = requiredString(arguments, "path", "MISSING_PATH");
		int startLine = integer(arguments.get("start_line"), 1);
		int limit = integer(arguments.get("limit"), 200);
		if (startLine <= 0) {
			throw protocolError("INVALID_START_LINE", "start_line must be a positive integer");
		}
		if (limit <= 0) {
			throw protocolError("INVALID_LIMIT", "limit must be a positive integer");
		}
		FragmentSearch fragment = getIndexedFragment(artifactId, path, namespaces);
		String content = fragment.getContent() == null ? "" : fragment.getContent();
		String[] lines = content.split("\\r?\\n", -1);
		int total = lines.length;
		Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
		structuredContent.put("path", path);
		structuredContent.put("artifact_id", artifactId);
		structuredContent.put("start_line", startLine);
		structuredContent.put("total", total);
		if (startLine > total) {
			structuredContent.put("count", 0);
			structuredContent.put("content", "");
			structuredContent.put("code", "EMPTY_RANGE");
			structuredContent.put("message", "start_line exceeds total lines");
			return structuredContent;
		}
		int from = startLine - 1;
		int to = Math.min(lines.length, from + limit);
		StringBuilder builder = new StringBuilder();
		for (int i = from; i < to; i++) {
			if (builder.length() > 0) {
				builder.append('\n');
			}
			builder.append(lines[i]);
		}
		structuredContent.put("count", to - from);
		structuredContent.put("content", builder.toString());
		return structuredContent;
	}

	private EditArtifactResult writeArtifact(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration, boolean preview) {
		List<String> namespaces = resolveNamespaces(configuration, null, meta);
		String artifactId = requiredString(arguments, "artifact_id", "MISSING_ARTIFACT_ID");
		String path = requiredString(arguments, "path", "MISSING_PATH");
		String content = requiredString(arguments, "content", "MISSING_CONTENT");
		String mode = string(arguments.get("mode"));
		if (mode == null) {
			mode = "overwrite";
		}
		if (!"overwrite".equals(mode) && !"append".equals(mode) && !"prepend".equals(mode)) {
			throw protocolError("INVALID_MODE", "mode must be overwrite, append, or prepend");
		}
		if (!isAllowedNamespace(artifactId, namespaces)) {
			throw protocolError("INVALID_PATH", "invalid path " + artifactId + ": path outside allowed namespace");
		}
		Node currentNode = server.getRepository().getNode(artifactId);
		if (currentNode == null) {
			throw protocolError("INVALID_PATH", "invalid path " + artifactId + ": artifact not found");
		}
		Artifact currentArtifact;
		ArtifactFragmentManager<Artifact> currentManager;
		ArtifactFragment currentFragment;
		String before;
		String after;
		Map<String, Object> operation = new LinkedHashMap<String, Object>();
		try {
			currentArtifact = (Artifact) currentNode.getArtifact();
			currentManager = EAIRepositoryUtils.getArtifactFragmentManager(currentArtifact);
			if (currentManager == null) {
				throw protocolError("INVALID_PATH", "invalid path " + artifactId + ": no fragment manager found");
			}
			currentFragment = findEditableFragment(currentManager, currentArtifact, artifactId, path);
			before = currentFragment.getContent();
			after = "append".equals(mode) ? before + content : "prepend".equals(mode) ? content + before : content;
			operation.put("artifactId", artifactId);
			operation.put("path", path);
			operation.put("artifact", currentArtifact);
			operation.put("manager", currentManager);
			operation.put("before", before);
			operation.put("after", after);
		}
		catch (Exception e) {
			Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
			structuredContent.put("path", path);
			structuredContent.put("mode", mode);
			structuredContent.put("preview", preview);
			structuredContent.put("updated", false);
			structuredContent.put("message", e.getMessage() == null ? e.getClass().getName() : e.getMessage());
			structuredContent.put("isError", true);
			ensureStaticReviewResource();
			return new EditArtifactResult(structuredContent, REVIEW_RESOURCE_URI);
		}
		Map<String, Object> update = new LinkedHashMap<String, Object>();
		update.put("artifactId", artifactId);
		update.put("path", path);
		if (preview) {
			update.put("updated", false);
			update.put("preview", true);
		}
		else {
			try {
				List<Validation<?>> validations = currentManager.updateFragment(currentArtifact, path, before, after);
				update.put("updated", !hasErrors(validations));
				if (validations != null && !validations.isEmpty()) {
					update.put("validations", validationMaps(validations));
				}
				if (hasErrors(validations)) {
					update.put("error", buildValidationMessage(validations));
				}
				else {
					reloadArtifactAfterMcpUpdate(artifactId);
					notifyCollaborationReload(artifactId);
				}
			}
			catch (Exception e) {
				update.put("updated", false);
				update.put("error", e.getMessage() == null ? e.getClass().getName() : e.getMessage());
			}
		}
		Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
		structuredContent.put("path", path);
		structuredContent.put("mode", mode);
		structuredContent.put("original", before);
		structuredContent.put("new", after);
		structuredContent.put("diff", buildFallbackDiff(Arrays.asList(operation)));
		structuredContent.put("preview", preview);
		structuredContent.put("updated", Boolean.TRUE.equals(update.get("updated")));
		if (update.get("error") != null) {
			structuredContent.put("message", update.get("error"));
			structuredContent.put("isError", true);
		}
		else {
			structuredContent.put("isError", false);
		}
		if (update.get("validations") != null) {
			structuredContent.put("validations", update.get("validations"));
		}
		ensureStaticReviewResource();
		return new EditArtifactResult(structuredContent, REVIEW_RESOURCE_URI);
	}

	private void reloadArtifactAfterMcpUpdate(String artifactId) {
		server.getRepository().reload(artifactId);
	}

	private void notifyCollaborationReload(String artifactId) {
		if (server.getCollaborationListener() != null) {
			server.getCollaborationListener().notifyArtifactReload(artifactId, "MCP updated");
		}
	}

	private List<MCPFragmentSearchResult> search(MCPToolCallInput input, Map<String, Object> meta, MCPConfiguration configuration) {
		FragmentIndexService service = server.getFragmentIndexService();
		if (service == null) {
			throw new HTTPException(503, "Fragment index is unavailable");
		}
		String pattern = input == null ? null : input.getPattern();
		if (pattern == null || pattern.trim().isEmpty()) {
			throw new HTTPException(400, "The pattern is required");
		}
		int before = number(input == null ? null : input.getBeforeContext());
		int after = number(input == null ? null : input.getAfterContext());
		if (input != null && input.getContext() != null) {
			before = input.getContext();
			after = input.getContext();
		}
		List<String> namespaces = resolveNamespaces(configuration, input == null ? null : input.getNamespace(), meta);
		List<FragmentSearch> search = service.search(pattern, input == null ? null : input.getGlob(), namespaces, before, after, 0);
		List<MCPFragmentSearchResult> results = new ArrayList<MCPFragmentSearchResult>();
		for (FragmentSearch fragment : search) {
			results.add(new MCPFragmentSearchResult(fragment.getArtifactId(), fragment.getPath(), fragment.getArtifactType(), fragment.getFragmentType(), fragment.getContentType(), fragment.getProperties(), fragment.isEditable(), fragment.isRemovable(), groupMatches(fragment.getMatches())));
		}
		return results;
	}

	private Map<String, Object> optimizeResults(String pattern, List<MCPFragmentSearchResult> results) {
		int totalResults = results.size();
		int totalMatches = countMatches(results);
		String mode = "full";
		boolean truncated = false;
		Object output = results;
		if (estimateStructuredContentSize(pattern, results, totalResults, totalMatches, mode, truncated) > MAX_RESULT_BYTES) {
			List<Map<String, Object>> reduced = reduceResults(results);
			mode = "reduced";
			truncated = true;
			output = reduced;
			if (estimateStructuredContentSize(pattern, reduced, totalResults, totalMatches, mode, truncated) > MAX_RESULT_BYTES) {
				List<Map<String, Object>> summary = summarizeResults(results);
				summary = reduceSummary(pattern, summary, totalResults, totalMatches);
				mode = "summary";
				output = summary;
			}
		}
		Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
		structuredContent.put("results", output);
		structuredContent.put("pattern", pattern);
		structuredContent.put("count", output instanceof List ? ((List<?>) output).size() : 0);
		structuredContent.put("total_results", totalResults);
		structuredContent.put("total_matches", totalMatches);
		structuredContent.put("truncated", truncated);
		structuredContent.put("mode", mode);
		return structuredContent;
	}

	private String buildEditSummaryText(Map<String, Object> structuredContent) {
		Object updatedCount = structuredContent.get("updated_count");
		Object failedCount = structuredContent.get("failed_count");
		boolean preview = isPreview(structuredContent.get("preview"));
		return (preview ? "Previewed " : "Updated ") + updatedCount + " fragments, " + failedCount + " failed";
	}

	private String buildWriteSummaryText(Map<String, Object> structuredContent) {
		boolean preview = isPreview(structuredContent.get("preview"));
		return (preview ? "Previewed " : "Wrote ") + structuredContent.get("path");
	}

	private String buildFindSummaryText(Map<String, Object> structuredContent) {
		Number count = (Number) structuredContent.get("count");
		Boolean truncated = (Boolean) structuredContent.get("truncated");
		Number limit = (Number) structuredContent.get("limit");
		if (Boolean.TRUE.equals(truncated)) {
			return "Found " + count + " fragment(s). Results truncated at limit " + limit + ".";
		}
		return "Found " + count + " fragment(s).";
	}

	private String buildReadSummaryText(Map<String, Object> structuredContent) {
		Number count = (Number) structuredContent.get("count");
		Number total = (Number) structuredContent.get("total");
		Number startLine = (Number) structuredContent.get("start_line");
		String path = (String) structuredContent.get("path");
		if ("EMPTY_RANGE".equals(structuredContent.get("code"))) {
			return "No lines returned from " + path + ": start_line " + startLine + " exceeds total " + total + ".";
		}
		return "Read " + count + " line(s) from " + path + " (start line " + startLine + ", total " + total + ").";
	}

	private boolean isPreview(Object value) {
		Object unwrapped = unwrap(value);
		if (unwrapped instanceof Boolean) {
			return ((Boolean) unwrapped).booleanValue();
		}
		if (unwrapped instanceof String) {
			return "true".equalsIgnoreCase(((String) unwrapped).trim());
		}
		return false;
	}

	private String buildSummaryText(Map<String, Object> structuredContent) {
		Number totalResults = (Number) structuredContent.get("total_results");
		String mode = (String) structuredContent.get("mode");
		if (totalResults == null || totalResults.intValue() == 0) {
			return "No results found";
		}
		StringBuilder builder = new StringBuilder();
		builder.append("Found ").append(totalResults.intValue()).append(" results");
		if (!"full".equals(mode)) {
			builder.append(" (").append(mode).append(" output)");
		}
		return builder.toString();
	}

	private int countMatches(List<MCPFragmentSearchResult> results) {
		int count = 0;
		for (MCPFragmentSearchResult result : results) {
			count += result.getMatches() == null ? 0 : result.getMatches().size();
		}
		return count;
	}

	private List<String> groupMatches(List<String> matches) {
		if (matches == null || matches.isEmpty()) {
			return Collections.emptyList();
		}
		List<String> grouped = new ArrayList<String>();
		StringBuilder builder = new StringBuilder();
		for (String line : matches) {
			if (line != null && "--".equals(line.trim())) {
				if (builder.length() > 0) {
					grouped.add(builder.toString());
					builder.setLength(0);
				}
				continue;
			}
			if (line == null) {
				continue;
			}
			if (builder.length() > 0) {
				builder.append('\n');
			}
			builder.append(line);
		}
		if (builder.length() > 0) {
			grouped.add(builder.toString());
		}
		return grouped;
	}

	private List<Map<String, Object>> reduceResults(List<MCPFragmentSearchResult> results) {
		List<Map<String, Object>> reduced = new ArrayList<Map<String, Object>>();
		for (MCPFragmentSearchResult result : results) {
			Map<String, Object> single = new LinkedHashMap<String, Object>();
			single.put("artifactId", result.getArtifactId());
			single.put("path", result.getPath());
			single.put("artifactType", result.getArtifactType());
			single.put("fragmentType", result.getFragmentType());
			single.put("contentType", result.getContentType());
			single.put("properties", result.getProperties());
			single.put("editable", result.isEditable());
			single.put("removable", result.isRemovable());
			single.put("matchCount", result.getMatches() == null ? 0 : result.getMatches().size());
			reduced.add(single);
		}
		return reduced;
	}

	private List<Map<String, Object>> summarizeResults(List<MCPFragmentSearchResult> results) {
		List<Map<String, Object>> summary = new ArrayList<Map<String, Object>>();
		for (MCPFragmentSearchResult result : results) {
			Map<String, Object> single = new LinkedHashMap<String, Object>();
			single.put("artifactId", result.getArtifactId());
			single.put("path", result.getPath());
			single.put("count", result.getMatches() == null ? 0 : result.getMatches().size());
			summary.add(single);
		}
		return summary;
	}

	private List<Map<String, Object>> reduceSummary(String pattern, List<Map<String, Object>> summary, int totalResults, int totalMatches) {
		if (estimateStructuredContentSize(pattern, summary, totalResults, totalMatches, "summary", true) <= MAX_RESULT_BYTES) {
			return summary;
		}
		Collections.sort(summary, new Comparator<Map<String, Object>>() {
			@Override
			public int compare(Map<String, Object> left, Map<String, Object> right) {
				int leftCount = ((Number) left.get("count")).intValue();
				int rightCount = ((Number) right.get("count")).intValue();
				return Integer.compare(rightCount, leftCount);
			}
		});
		int low = Math.min(SUMMARY_TOP, summary.size());
		int high = summary.size();
		int best = 0;
		while (low <= high) {
			int middle = low + (high - low) / 2;
			List<Map<String, Object>> candidate = new ArrayList<Map<String, Object>>(summary.subList(0, middle));
			if (estimateStructuredContentSize(pattern, candidate, totalResults, totalMatches, "summary", true) <= MAX_RESULT_BYTES) {
				best = middle;
				low = middle + 1;
			}
			else {
				high = middle - 1;
			}
		}
		if (best == 0) {
			best = Math.min(SUMMARY_TOP, summary.size());
		}
		return new ArrayList<Map<String, Object>>(summary.subList(0, best));
	}

	private int estimateStructuredContentSize(String pattern, Object results, int totalResults, int totalMatches, String mode, boolean truncated) {
		Map<String, Object> candidate = new LinkedHashMap<String, Object>();
		candidate.put("results", results);
		candidate.put("pattern", pattern);
		candidate.put("count", results instanceof List ? ((List<?>) results).size() : 0);
		candidate.put("total_results", totalResults);
		candidate.put("total_matches", totalMatches);
		candidate.put("truncated", truncated);
		candidate.put("mode", mode);
		try {
			return marshal(candidate).length;
		}
		catch (IOException e) {
			return Integer.MAX_VALUE;
		}
	}


	private int number(Integer value) {
		return value == null ? 0 : Math.max(0, value.intValue());
	}

	private MCPConfiguration resolveSessionConfiguration(HTTPRequest request, boolean failOnMissing) {
		purgeExpiredSessions();
		String sessionId = header(request, MCP_SESSION_ID);
		if (sessionId == null || sessionId.trim().isEmpty()) {
			if (failOnMissing) {
				return null;
			}
			return null;
		}
		MCPSession session = sessions().get(sessionId);
		if (session == null || session.isExpired()) {
			sessions().remove(sessionId);
			throw new HTTPException(404, "Unknown MCP session id: " + sessionId);
		}
		session.touch();
		sessions().put(sessionId, session);
		return session.getConfiguration();
	}

	private void storeSession(String sessionId, MCPConfiguration configuration) {
		purgeExpiredSessions();
		sessions().put(sessionId, new MCPSession(configuration));
	}

	private void purgeExpiredSessions() {
		List<String> expired = new ArrayList<String>();
		for (Map.Entry<String, MCPSession> entry : sessions().entrySet()) {
			if (entry.getValue() == null || entry.getValue().isExpired()) {
				expired.add(entry.getKey());
			}
		}
		for (String key : expired) {
			sessions().remove(key);
		}
	}

	private ClusterMap<String, MCPSession> sessions() {
		return server.getCluster().map(SESSION_MAP);
	}

	private String header(HTTPRequest request, String name) {
		if (request == null || request.getContent() == null || request.getContent().getHeaders() == null) {
			return null;
		}
		for (Header header : request.getContent().getHeaders()) {
			if (header != null && name.equalsIgnoreCase(header.getName())) {
				return header.getValue();
			}
		}
		return null;
	}

	private void applyInitializeConfiguration(MCPConfiguration configuration, Map<String, Object> params) {
		if (params == null) {
			return;
		}
		Map<String, Object> capabilities = map(params.get("capabilities"));
		Map<String, Object> experimental = capabilities == null ? null : map(capabilities.get("experimental"));
		Map<String, Object> config = experimental == null ? null : map(experimental.get("configuration"));
		if (config == null) {
			return;
		}
		applyConfiguration(configuration, config, false);
	}

	private void applyConfiguration(MCPConfiguration configuration, Map<String, Object> values, boolean policy) {
		for (Map.Entry<String, Object> entry : values.entrySet()) {
			String key = entry.getKey();
			if ("namespace".equals(key)) {
				configuration.setNamespace(stringList(entry.getValue(), key));
			}
			else if (policy) {
				throw new HTTPException(400, "Unknown policy key: " + key);
			}
			else {
				throw new HTTPException(400, "Unknown configuration key: " + key);
			}
		}
	}

	private List<String> resolveNamespaces(MCPConfiguration configuration, List<String> requestedNamespaces, Map<String, Object> meta) {
		List<String> namespaces = filterValues(configuration == null ? null : configuration.getNamespace());
		Map<String, Object> policy = meta == null ? null : map(meta.get("policy"));
		if (policy != null) {
			List<String> policyNamespaces = resolvePolicyNamespaces(policy);
			namespaces = intersectNamespaces(namespaces, policyNamespaces);
		}
		return intersectNamespaces(namespaces, filterValues(requestedNamespaces));
	}

	private List<String> resolvePolicyNamespaces(Map<String, Object> policy) {
		List<String> namespaces = Collections.emptyList();
		boolean configured = false;
		Map<String, Object> policyConfiguration = map(policy.get("configuration"));
		if (policyConfiguration != null) {
			MCPConfiguration configuration = new MCPConfiguration();
			applyPolicyConfiguration(configuration, policyConfiguration);
			namespaces = filterValues(configuration.getNamespace());
			configured = true;
		}
		if (policy.containsKey("namespace")) {
			List<String> directNamespaces = filterValues(stringList(policy.get("namespace"), "policy.namespace"));
			if (configured) {
				namespaces = intersectNamespaces(namespaces, directNamespaces);
			}
			else {
				namespaces = directNamespaces;
			}
		}
		return namespaces;
	}

	private List<String> intersectNamespaces(List<String> left, List<String> right) {
		if (left == null || left.isEmpty()) {
			return Collections.emptyList();
		}
		if (right == null || right.isEmpty()) {
			return Collections.emptyList();
		}
		List<String> merged = new ArrayList<String>();
		for (String leftNamespace : left) {
			for (String rightNamespace : right) {
				String narrower = narrowNamespace(leftNamespace, rightNamespace);
				if (narrower != null && !merged.contains(narrower)) {
					merged.add(narrower);
				}
			}
		}
		return merged;
	}

	private void applyPolicyConfiguration(MCPConfiguration configuration, Map<String, Object> values) {
		for (Map.Entry<String, Object> entry : values.entrySet()) {
			if ("namespace".equals(entry.getKey())) {
				configuration.setNamespace(stringList(entry.getValue(), "policy.configuration.namespace"));
			}
			else {
				throw new HTTPException(400, "Unknown policy configuration key: " + entry.getKey());
			}
		}
	}

	private String narrowNamespace(String left, String right) {
		if (left.equals(right) || left.startsWith(right + ".")) {
			return left;
		}
		if (right.startsWith(left + ".")) {
			return right;
		}
		return null;
	}

	private List<String> filterValues(List<String> values) {
		if (values == null || values.isEmpty()) {
			return Collections.emptyList();
		}
		List<String> filtered = new ArrayList<String>();
		for (String value : values) {
			if (value != null) {
				value = value.trim();
				if (!value.isEmpty() && !filtered.contains(value)) {
					filtered.add(value);
				}
			}
		}
		return filtered;
	}

	@SuppressWarnings("unchecked")
	private List<String> stringList(Object object, String label) {
		Object unwrap = unwrap(object);
		if (unwrap == null) {
			return Collections.emptyList();
		}
		if (!(unwrap instanceof List)) {
			throw new HTTPException(400, label + " must be an array of strings");
		}
		List<String> values = new ArrayList<String>();
		for (Object single : (List<Object>) unwrap) {
			if (single != null) {
				values.add(single.toString());
			}
		}
		return values;
	}

	static long getSessionTimeout() {
		return SESSION_TIMEOUT;
	}

	private Map<String, Object> configSchema() {
		Map<String, Object> schema = new LinkedHashMap<String, Object>();
		schema.put("$schema", "http://json-schema.org/draft-07/schema#");
		schema.put("title", "nabu-mcp configuration");
		schema.put("type", "object");
		schema.put("additionalProperties", false);
		Map<String, Object> properties = new LinkedHashMap<String, Object>();
		Map<String, Object> namespace = schema("array");
		namespace.put("items", schema("string"));
		namespace.put("description", "Restrict accessible artifact namespaces. Matches the exact namespace and all descendant artifact ids.");
		namespace.put("scope", "any");
		namespace.put("audience", "any");
		properties.put("namespace", namespace);
		schema.put("properties", properties);
		return schema;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> map(Object object) {
		Object unwrap = unwrap(object);
		return unwrap instanceof Map ? (Map<String, Object>) unwrap : null;
	}

	private String string(Object object) {
		Object unwrap = unwrap(object);
		return unwrap == null ? null : unwrap.toString();
	}

	private void ensureStaticReviewResource() {
		if (reviewResources().containsKey(REVIEW_RESOURCE_URI)) {
			return;
		}
		storeReviewResource(REVIEW_RESOURCE_URI, "diff.html", "text/html", buildStaticReviewHtml().getBytes(StandardCharsets.UTF_8));
	}

	private String buildStaticReviewHtml() {
		StringBuilder builder = new StringBuilder();
		builder.append("<html><head><title>Review</title><style>");
		builder.append("body{font-family:ui-sans-serif,system-ui,sans-serif;margin:0;padding:0;background:#0d1117;color:#c9d1d9;}header{padding:16px 20px;border-bottom:1px solid #30363d;}main{padding:20px;}pre{margin:0;white-space:pre-wrap;word-break:break-word;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;line-height:1.5;} .block{border:1px solid #30363d;border-radius:6px;overflow:hidden;margin-bottom:16px;} .label{padding:8px 12px;background:#161b22;border-bottom:1px solid #30363d;color:#8b949e;font-size:12px;} .pane{padding:12px;background:#0d1117;} .diff .add{background:#033a16;} .diff .del{background:#4c0519;} .meta{color:#8b949e;font-size:12px;margin-bottom:16px;}");
		builder.append("</style></head><body><header><h2 style='margin:0'>Artifact Review</h2></header><main id='app'></main><script>");
		builder.append("function esc(v){return (v||'').replace(/[&<>]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;'}[c];});}");
		builder.append("function lines(v){return (v||'').split('\\n').map(function(line){var cls=''; if(line.startsWith('+') && !line.startsWith('+++')) cls='add'; else if(line.startsWith('-') && !line.startsWith('---')) cls='del'; return '<div class=\"'+cls+'\">'+esc(line)+'</div>';}).join('');}");
		builder.append("var data=(window.mcp&&window.mcp.structuredContent)||{}; var updates=data.updates||[]; var app=document.getElementById('app'); var html=''; html += '<div class=\"meta\">'+(data.preview ? 'Preview' : 'Review')+'</div>'; if(data.path){ html += '<div class=\"meta\">'+esc(data.path)+'</div>'; } if(data.original!==undefined){ html += '<div class=\"block\"><div class=\"label\">Original</div><div class=\"pane\"><pre>'+esc(data.original)+'</pre></div></div>'; } if(data.new!==undefined){ html += '<div class=\"block\"><div class=\"label\">New</div><div class=\"pane\"><pre>'+esc(data.new)+'</pre></div></div>'; } if(data.diff!==undefined){ html += '<div class=\"block\"><div class=\"label\">Diff</div><div class=\"pane diff\"><pre>'+lines(data.diff)+'</pre></div></div>'; } if(!data.path && updates.length){ html += '<div class=\"block\"><div class=\"label\">Updates</div><div class=\"pane\"><pre>'+esc(JSON.stringify(updates, null, 2))+'</pre></div></div>'; } app.innerHTML = html;");
		builder.append("</script></body></html>");
		return builder.toString();
	}

	private String buildFallbackDiff(List<Map<String, Object>> operations) {
		StringBuilder builder = new StringBuilder();
		for (Map<String, Object> operation : operations) {
			builder.append("--- a/").append(operation.get("artifactId")).append("/").append(operation.get("path")).append("\n");
			builder.append("+++ b/").append(operation.get("artifactId")).append("/").append(operation.get("path")).append("\n");
			builder.append("@@\n");
			builder.append((String) operation.get("before")).append("\n");
			builder.append("@@\n");
			builder.append((String) operation.get("after")).append("\n");
		}
		return builder.toString();
	}

	private void storeReviewResource(String uri, String name, String mimeType, byte[] content) {
		Map<String, Object> resource = new LinkedHashMap<String, Object>();
		resource.put("uri", uri);
		resource.put("name", name);
		resource.put("mimeType", mimeType);
		resource.put("content", content);
		reviewResources().put(uri, resource);
	}

	private List<Map<String, Object>> listResources() {
		List<Map<String, Object>> resources = new ArrayList<Map<String, Object>>();
		for (Map.Entry<String, Map<String, Object>> entry : reviewResources().entrySet()) {
			Map<String, Object> resource = entry.getValue();
			if (resource == null) {
				continue;
			}
			Map<String, Object> listed = new LinkedHashMap<String, Object>();
			listed.put("uri", resource.get("uri"));
			listed.put("name", resource.get("name"));
			listed.put("mimeType", resource.get("mimeType"));
			resources.add(listed);
		}
		return resources;
	}

	private List<Map<String, Object>> readResource(String uri) {
		if (uri == null || uri.trim().isEmpty()) {
			throw new HTTPException(400, "uri is required");
		}
		Map<String, Object> resource = reviewResources().get(uri);
		if (resource == null) {
			throw new HTTPException(404, "resource not found: " + uri);
		}
		Map<String, Object> content = new LinkedHashMap<String, Object>();
		content.put("uri", resource.get("uri"));
		content.put("mimeType", resource.get("mimeType"));
		content.put("text", new String((byte[]) resource.get("content"), StandardCharsets.UTF_8));
		return Arrays.asList(content);
	}

	private ClusterMap<String, Map<String, Object>> reviewResources() {
		return server.getCluster().map(REVIEW_RESOURCE_MAP);
	}

	private List<Map<String, String>> textContent(String textValue) {
		List<Map<String, String>> content = new ArrayList<Map<String, String>>();
		Map<String, String> text = new LinkedHashMap<String, String>();
		text.put("type", "text");
		text.put("text", textValue);
		content.add(text);
		return content;
	}

	private Map<String, Object> buildToolMeta(String resourceUri) {
		Map<String, Object> meta = new LinkedHashMap<String, Object>();
		if (resourceUri != null && !resourceUri.trim().isEmpty()) {
			Map<String, Object> ui = new LinkedHashMap<String, Object>();
			ui.put("resourceUri", resourceUri);
			meta.put("ui", ui);
		}
		return meta.isEmpty() ? null : meta;
	}

	private Map<String, Object> error(int code, String message) {
		Map<String, Object> error = new LinkedHashMap<String, Object>();
		error.put("code", code);
		error.put("message", message);
		return error;
	}

	private Map<String, Object> searchOutputSchema() {
		Map<String, Object> schema = new LinkedHashMap<String, Object>();
		schema.put("type", "object");
		Map<String, Object> properties = new LinkedHashMap<String, Object>();
		Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
		structuredContent.put("type", "object");
		Map<String, Object> structuredProperties = new LinkedHashMap<String, Object>();
		structuredProperties.put("results", schema("array"));
		structuredProperties.put("pattern", schema("string"));
		structuredProperties.put("count", schema("integer"));
		structuredProperties.put("total_results", schema("integer"));
		structuredProperties.put("total_matches", schema("integer"));
		structuredProperties.put("truncated", schema("boolean"));
		structuredProperties.put("mode", schema("string"));
		structuredContent.put("properties", structuredProperties);
		properties.put("structuredContent", structuredContent);
		Map<String, Object> content = schema("array");
		Map<String, Object> contentItems = new LinkedHashMap<String, Object>();
		contentItems.put("type", "object");
		Map<String, Object> contentProperties = new LinkedHashMap<String, Object>();
		contentProperties.put("type", schema("string"));
		contentProperties.put("text", schema("string"));
		contentItems.put("properties", contentProperties);
		contentItems.put("required", Arrays.asList("type", "text"));
		content.put("items", contentItems);
		properties.put("content", content);
		schema.put("properties", properties);
		return schema;
	}

	private Map<String, Object> editOutputSchema() {
		Map<String, Object> schema = new LinkedHashMap<String, Object>();
		schema.put("type", "object");
		Map<String, Object> properties = new LinkedHashMap<String, Object>();
		Map<String, Object> meta = new LinkedHashMap<String, Object>();
		meta.put("type", "object");
		Map<String, Object> metaProperties = new LinkedHashMap<String, Object>();
		Map<String, Object> ui = new LinkedHashMap<String, Object>();
		ui.put("type", "object");
		Map<String, Object> uiProperties = new LinkedHashMap<String, Object>();
		uiProperties.put("resourceUri", schema("string"));
		ui.put("properties", uiProperties);
		metaProperties.put("ui", ui);
		meta.put("properties", metaProperties);
		properties.put("_meta", meta);
		Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
		structuredContent.put("type", "object");
		Map<String, Object> structuredProperties = new LinkedHashMap<String, Object>();
		structuredProperties.put("path", schema("string"));
		structuredProperties.put("match_count", schema("integer"));
		structuredProperties.put("original", schema("string"));
		structuredProperties.put("new", schema("string"));
		structuredProperties.put("diff", schema("string"));
		structuredProperties.put("count", schema("integer"));
		structuredProperties.put("updated_count", schema("integer"));
		structuredProperties.put("failed_count", schema("integer"));
		structuredProperties.put("isError", schema("boolean"));
		structuredProperties.put("preview", schema("boolean"));
		Map<String, Object> updates = schema("array");
		Map<String, Object> updateItem = new LinkedHashMap<String, Object>();
		updateItem.put("type", "object");
		Map<String, Object> updateProperties = new LinkedHashMap<String, Object>();
		updateProperties.put("artifactId", schema("string"));
		updateProperties.put("path", schema("string"));
		updateProperties.put("match_count", schema("integer"));
		updateProperties.put("updated", schema("boolean"));
		updateProperties.put("error", schema("string"));
		updateProperties.put("validations", schema("array"));
		updateItem.put("properties", updateProperties);
		updates.put("items", updateItem);
		structuredProperties.put("updates", updates);
		structuredContent.put("properties", structuredProperties);
		properties.put("structuredContent", structuredContent);
		Map<String, Object> content = schema("array");
		Map<String, Object> contentItems = new LinkedHashMap<String, Object>();
		contentItems.put("type", "object");
		Map<String, Object> contentProperties = new LinkedHashMap<String, Object>();
		contentProperties.put("type", schema("string"));
		contentProperties.put("text", schema("string"));
		contentItems.put("properties", contentProperties);
		contentItems.put("required", Arrays.asList("type", "text"));
		content.put("items", contentItems);
		properties.put("content", content);
		schema.put("properties", properties);
		return schema;
	}

	private Map<String, Object> findOutputSchema() {
		Map<String, Object> schema = new LinkedHashMap<String, Object>();
		schema.put("type", "object");
		Map<String, Object> properties = new LinkedHashMap<String, Object>();
		Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
		structuredContent.put("type", "object");
		Map<String, Object> structuredProperties = new LinkedHashMap<String, Object>();
		structuredProperties.put("files", schema("array"));
		structuredProperties.put("count", schema("integer"));
		structuredProperties.put("total", schema("integer"));
		structuredProperties.put("limit", schema("integer"));
		structuredProperties.put("offset", schema("integer"));
		structuredProperties.put("truncated", schema("boolean"));
		structuredContent.put("properties", structuredProperties);
		properties.put("structuredContent", structuredContent);
		Map<String, Object> content = schema("array");
		Map<String, Object> contentItems = new LinkedHashMap<String, Object>();
		contentItems.put("type", "object");
		Map<String, Object> contentProperties = new LinkedHashMap<String, Object>();
		contentProperties.put("type", schema("string"));
		contentProperties.put("text", schema("string"));
		contentItems.put("properties", contentProperties);
		contentItems.put("required", Arrays.asList("type", "text"));
		content.put("items", contentItems);
		properties.put("content", content);
		schema.put("properties", properties);
		return schema;
	}

	private Map<String, Object> readOutputSchema() {
		Map<String, Object> schema = new LinkedHashMap<String, Object>();
		schema.put("type", "object");
		Map<String, Object> properties = new LinkedHashMap<String, Object>();
		Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
		structuredContent.put("type", "object");
		Map<String, Object> structuredProperties = new LinkedHashMap<String, Object>();
		structuredProperties.put("artifact_id", schema("string"));
		structuredProperties.put("path", schema("string"));
		structuredProperties.put("start_line", schema("integer"));
		structuredProperties.put("count", schema("integer"));
		structuredProperties.put("total", schema("integer"));
		structuredProperties.put("content", schema("string"));
		structuredProperties.put("code", schema("string"));
		structuredProperties.put("message", schema("string"));
		structuredContent.put("properties", structuredProperties);
		properties.put("structuredContent", structuredContent);
		Map<String, Object> content = schema("array");
		Map<String, Object> contentItems = new LinkedHashMap<String, Object>();
		contentItems.put("type", "object");
		Map<String, Object> contentProperties = new LinkedHashMap<String, Object>();
		contentProperties.put("type", schema("string"));
		contentProperties.put("text", schema("string"));
		contentItems.put("properties", contentProperties);
		contentItems.put("required", Arrays.asList("type", "text"));
		content.put("items", contentItems);
		properties.put("content", content);
		schema.put("properties", properties);
		return schema;
	}

	private Map<String, Object> writeOutputSchema() {
		Map<String, Object> schema = new LinkedHashMap<String, Object>();
		schema.put("type", "object");
		Map<String, Object> properties = new LinkedHashMap<String, Object>();
		Map<String, Object> meta = new LinkedHashMap<String, Object>();
		meta.put("type", "object");
		Map<String, Object> metaProperties = new LinkedHashMap<String, Object>();
		Map<String, Object> ui = new LinkedHashMap<String, Object>();
		ui.put("type", "object");
		Map<String, Object> uiProperties = new LinkedHashMap<String, Object>();
		uiProperties.put("resourceUri", schema("string"));
		ui.put("properties", uiProperties);
		metaProperties.put("ui", ui);
		meta.put("properties", metaProperties);
		properties.put("_meta", meta);
		Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
		structuredContent.put("type", "object");
		Map<String, Object> structuredProperties = new LinkedHashMap<String, Object>();
		structuredProperties.put("path", schema("string"));
		structuredProperties.put("mode", schema("string"));
		structuredProperties.put("original", schema("string"));
		structuredProperties.put("new", schema("string"));
		structuredProperties.put("diff", schema("string"));
		structuredProperties.put("preview", schema("boolean"));
		structuredProperties.put("updated", schema("boolean"));
		structuredProperties.put("isError", schema("boolean"));
		structuredProperties.put("message", schema("string"));
		structuredProperties.put("validations", schema("array"));
		structuredContent.put("properties", structuredProperties);
		properties.put("structuredContent", structuredContent);
		Map<String, Object> content = schema("array");
		Map<String, Object> contentItems = new LinkedHashMap<String, Object>();
		contentItems.put("type", "object");
		Map<String, Object> contentProperties = new LinkedHashMap<String, Object>();
		contentProperties.put("type", schema("string"));
		contentProperties.put("text", schema("string"));
		contentItems.put("properties", contentProperties);
		contentItems.put("required", Arrays.asList("type", "text"));
		content.put("items", contentItems);
		properties.put("content", content);
		schema.put("properties", properties);
		return schema;
	}

	private Map<String, Object> editSchema() {
		Map<String, Object> schema = new LinkedHashMap<String, Object>();
		schema.put("type", "array");
		schema.put("description", "List of exact find/replace edits to apply in order.");
		Map<String, Object> items = new LinkedHashMap<String, Object>();
		items.put("type", "object");
		Map<String, Object> properties = new LinkedHashMap<String, Object>();
		properties.put("artifact_id", propertySchema("string", "Artifact id containing the fragment."));
		properties.put("path", propertySchema("string", "Path to the fragment inside the artifact."));
		properties.put("find", propertySchema("string", "Exact text to find (must match exactly once)."));
		properties.put("replace", propertySchema("string", "Replacement text."));
		items.put("properties", properties);
		items.put("required", Arrays.asList("artifact_id", "path", "find", "replace"));
		schema.put("items", items);
		return schema;
	}

	private Map<String, Object> propertySchema(String type, String description) {
		Map<String, Object> schema = schema(type);
		schema.put("description", description);
		return schema;
	}

	private List<Map<String, String>> extractEdits(Map<String, Object> arguments) {
		if (arguments == null || !arguments.containsKey("edits")) {
			throw protocolError("MISSING_EDITS", "edits is required");
		}
		Object rawEdits = unwrap(arguments.get("edits"));
		if (!(rawEdits instanceof List)) {
			throw protocolError("INVALID_EDITS", "edits must be an array");
		}
		List<?> values = (List<?>) rawEdits;
		if (values.isEmpty()) {
			throw protocolError("EMPTY_EDITS", "edits is empty");
		}
		List<Map<String, String>> edits = new ArrayList<Map<String, String>>();
		for (int i = 0; i < values.size(); i++) {
			if (!(values.get(i) instanceof Map)) {
				throw protocolError("INVALID_EDITS", "edit must be an object at index " + i);
			}
			Map<String, Object> edit = map(values.get(i));
			String artifactId = requiredString(edit, "artifact_id", "MISSING_ARTIFACT_ID");
			String path = requiredString(edit, "path", "MISSING_PATH");
			String find = requiredString(edit, "find", "MISSING_FIND");
			String replace = requiredString(edit, "replace", "MISSING_REPLACE");
			if (find.isEmpty()) {
				throw protocolError("FIND_EMPTY", "find text is empty at index " + i);
			}
			Map<String, String> normalized = new LinkedHashMap<String, String>();
			normalized.put("artifact_id", artifactId);
			normalized.put("path", path);
			normalized.put("find", find);
			normalized.put("replace", replace);
			edits.add(normalized);
		}
		return edits;
	}

	private String requiredString(Map<String, Object> values, String key, String code) {
		String value = values == null ? null : string(values.get(key));
		if (value == null) {
			throw protocolError(code, key + " is required");
		}
		return value;
	}

	private ArtifactFragment findEditableFragment(ArtifactFragmentManager<Artifact> manager, Artifact artifact, String artifactId, String path) {
		List<ArtifactFragment> fragments = manager.listFragments(artifact);
		if (fragments != null) {
			for (ArtifactFragment fragment : fragments) {
				if (fragment != null && path.equals(fragment.getPath())) {
					if (!fragment.isEditable()) {
						throw protocolError("INVALID_PATH", "invalid path " + artifactId + ": fragment is not editable");
					}
					return fragment;
				}
			}
		}
		throw protocolError("INVALID_PATH", "invalid path " + artifactId + ": fragment not found");
	}

	private boolean isAllowedNamespace(String artifactId, List<String> namespaces) {
		if (namespaces == null || namespaces.isEmpty()) {
			return true;
		}
		for (String namespace : namespaces) {
			if (artifactId.equals(namespace) || artifactId.startsWith(namespace + ".")) {
				return true;
			}
		}
		return false;
	}

	private boolean matchesFindPattern(String pattern, boolean glob, String artifactId, String path) {
		String candidate = artifactId + "/" + path;
		String normalizedPattern = glob ? globToRegex(pattern) : pattern;
		try {
			return candidate.matches(normalizedPattern) || path.matches(normalizedPattern) || artifactId.matches(normalizedPattern);
		}
		catch (Exception e) {
			throw protocolError("INVALID_PATTERN", "invalid pattern: " + pattern);
		}
	}

	private String globToRegex(String pattern) {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < pattern.length(); i++) {
			char character = pattern.charAt(i);
			if (character == '*') {
				builder.append(".*");
			}
			else if (character == '?') {
				builder.append('.');
			}
			else if ("\\.^$+{}[]()|".indexOf(character) >= 0) {
				builder.append('\\').append(character);
			}
			else {
				builder.append(character);
			}
		}
		return builder.toString();
	}

	private FragmentSearch getIndexedFragment(String artifactId, String path, List<String> namespaces) {
		FragmentIndexService service = server.getFragmentIndexService();
		if (service == null) {
			throw new HTTPException(503, "Fragment index is unavailable");
		}
		List<String> globs = Arrays.asList(path);
		List<FragmentSearch> fragments = service.search(".*", globs, namespaces, 0, 0, 0);
		for (FragmentSearch fragment : fragments) {
			if (artifactId.equals(fragment.getArtifactId()) && path.equals(fragment.getPath())) {
				return fragment;
			}
		}
		throw protocolError("INVALID_PATH", "invalid path " + artifactId + ": fragment not found");
	}

	private int integer(Object value, int defaultValue) {
		Object unwrapped = unwrap(value);
		if (unwrapped == null) {
			return defaultValue;
		}
		if (unwrapped instanceof Number) {
			return ((Number) unwrapped).intValue();
		}
		if (unwrapped instanceof String && ((String) unwrapped).trim().matches("-?\\d+")) {
			return Integer.parseInt(((String) unwrapped).trim());
		}
		throw protocolError("INVALID_NUMBER", "expected integer value");
	}

	private int countMatches(String content, String find) {
		int count = 0;
		int index = 0;
		while ((index = content.indexOf(find, index)) >= 0) {
			count++;
			index += find.length();
		}
		return count;
	}

	private boolean hasErrors(List<Validation<?>> validations) {
		if (validations == null) {
			return false;
		}
		for (Validation<?> validation : validations) {
			if (validation != null && validation.getSeverity() != null && validation.getSeverity().ordinal() >= Severity.ERROR.ordinal()) {
				return true;
			}
		}
		return false;
	}

	private String buildValidationMessage(List<Validation<?>> validations) {
		if (validations == null || validations.isEmpty()) {
			return "fragment update failed";
		}
		StringBuilder builder = new StringBuilder();
		for (Validation<?> validation : validations) {
			if (validation == null) {
				continue;
			}
			if (builder.length() > 0) {
				builder.append("; ");
			}
			builder.append(validation.getMessage());
		}
		return builder.length() == 0 ? "fragment update failed" : builder.toString();
	}

	private List<Map<String, Object>> validationMaps(List<Validation<?>> validations) {
		List<Map<String, Object>> mapped = new ArrayList<Map<String, Object>>();
		for (Validation<?> validation : validations) {
			if (validation == null) {
				continue;
			}
			Map<String, Object> single = new LinkedHashMap<String, Object>();
			single.put("severity", validation.getSeverity() == null ? null : validation.getSeverity().name());
			single.put("code", validation.getCode());
			single.put("message", validation.getMessage());
			single.put("description", validation.getDescription());
			single.put("context", validation.getContext());
			mapped.add(single);
		}
		return mapped;
	}

	private HTTPException protocolError(String code, String message) {
		return new HTTPException(400, message + " [" + code + "]");
	}

	private Map<String, Object> schema(String type) {
		Map<String, Object> schema = new LinkedHashMap<String, Object>();
		schema.put("type", type);
		return schema;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> parse(InputStream content) throws IOException, ParseException {
		JSONBinding binding = new JSONBinding(new MapTypeGenerator(), Charset.forName("UTF-8"));
		binding.setNormalize(false);
		binding.setEnableMapSupport(true);
		binding.setAllowDynamicElements(true);
		binding.setAddDynamicElementDefinitions(true);
		return (Map<String, Object>) unwrap(binding.unmarshal(content, new Window[0]));
	}

	private <T> T bind(Map<String, Object> content, Class<T> clazz) throws IOException, ParseException {
		if (content == null) {
			return null;
		}
		byte[] marshalled = marshal(content);
		JSONBinding binding = new JSONBinding((be.nabu.libs.types.api.ComplexType) be.nabu.libs.types.java.BeanResolver.getInstance().resolve(clazz), Charset.forName("UTF-8"));
		binding.setEnableMapSupport(true);
		binding.setAllowDynamicElements(true);
		return clazz.cast(((be.nabu.libs.types.java.BeanInstance<?>) binding.unmarshal(IOUtils.toInputStream(IOUtils.wrap(marshalled, true)), new Window[0])).getUnwrapped());
	}

	@SuppressWarnings("rawtypes")
	private byte[] marshal(Object content) throws IOException {
		ComplexContent wrapped;
		if (content instanceof Map) {
			wrapped = new MapContentWrapper().wrap((Map) content);
		}
		else {
			wrapped = new BeanInstance(content);
		}
		JSONBinding binding = new JSONBinding(wrapped.getType(), Charset.forName("UTF-8"));
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		binding.marshal(output, wrapped);
		return output.toByteArray();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Object unwrap(Object object) {
		if (object instanceof MapContent) {
			Map<String, Object> unwrapped = new LinkedHashMap<String, Object>();
			Map<String, Object> content = ((MapContent) object).getContent();
			for (Map.Entry<String, Object> entry : content.entrySet()) {
				unwrapped.put(entry.getKey(), unwrap(entry.getValue()));
			}
			return unwrapped;
		}
		if (object instanceof Map) {
			Map<String, Object> unwrapped = new LinkedHashMap<String, Object>();
			for (Map.Entry entry : ((Map<?, ?>) object).entrySet()) {
				unwrapped.put(entry.getKey().toString(), unwrap(entry.getValue()));
			}
			return unwrapped;
		}
		if (object instanceof List) {
			List<Object> unwrapped = new ArrayList<Object>();
			for (Object single : (List<?>) object) {
				unwrapped.add(unwrap(single));
			}
			return unwrapped;
		}
		return object;
	}

	private PlainMimeContentPart json(Map<String, Object> response, String sessionId) throws IOException {
		byte[] content = marshal(response);
		List<Header> headers = new ArrayList<Header>();
		headers.add(new MimeHeader("Content-Length", Integer.toString(content.length)));
		headers.add(new MimeHeader("Content-Type", MediaType.APPLICATION_JSON));
		headers.add(new MimeHeader("MCP-Protocol-Version", MCP_VERSION));
		if (sessionId != null && !sessionId.trim().isEmpty()) {
			headers.add(new MimeHeader(MCP_SESSION_ID, sessionId));
		}
		return new PlainMimeContentPart(null, IOUtils.wrap(content, true), headers.toArray(new Header[headers.size()]));
	}
}
