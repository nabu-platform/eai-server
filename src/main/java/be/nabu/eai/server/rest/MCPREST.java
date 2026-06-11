package be.nabu.eai.server.rest;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;

import be.nabu.eai.repository.CollectionImpl;
import be.nabu.eai.repository.EAIRepositoryUtils;
import be.nabu.eai.repository.EAIResourceRepository;
import be.nabu.eai.repository.api.ArtifactFragmentManager;
import be.nabu.eai.repository.api.CreatableArtifactFragmentManager;
import be.nabu.eai.repository.api.DynamicArtifactFragmentManager;
import be.nabu.eai.repository.api.Entry;
import be.nabu.eai.repository.api.ReviewableArtifactFragmentManager;
import be.nabu.eai.repository.api.ReviewableArtifactFragmentManager.ReviewResource;
import be.nabu.eai.repository.impl.StreamHiderContent;
import be.nabu.eai.repository.util.SystemPrincipal;
import be.nabu.eai.repository.api.ArtifactFragmentManager.ArtifactFragment;
import be.nabu.eai.repository.api.ExtensibleEntry;
import be.nabu.eai.repository.api.Node;
import be.nabu.eai.repository.api.ResourceEntry;
import be.nabu.libs.artifacts.api.Artifact;
import be.nabu.libs.authentication.impl.ImpersonateToken;
import be.nabu.eai.repository.resources.RepositoryEntry;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import be.nabu.eai.server.Server;
import be.nabu.eai.server.documentation.DocumentationCatalogService;
import be.nabu.eai.server.documentation.DocumentationSearch;
import be.nabu.eai.server.fragments.FragmentIndexService;
import be.nabu.eai.server.fragments.FragmentSearch;
import be.nabu.eai.server.fragments.MCPUtils;
import be.nabu.libs.authentication.api.Token;
import be.nabu.libs.cluster.api.ClusterMap;
import be.nabu.libs.services.ServiceRuntime;
import be.nabu.libs.services.api.DefinedService;
import be.nabu.libs.services.api.ExecutionContext;
import be.nabu.libs.services.api.FeaturedExecutionContext;
import be.nabu.libs.services.api.Service;
import be.nabu.libs.services.api.ServiceException;
import be.nabu.libs.services.api.ServiceResult;
import be.nabu.libs.services.api.ServiceRuntimeTracker;
import be.nabu.libs.http.HTTPException;
import be.nabu.libs.http.api.HTTPRequest;
import be.nabu.libs.resources.api.ManageableContainer;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.binding.api.Window;
import be.nabu.libs.types.binding.json.JSONBinding;
import be.nabu.libs.types.binding.xml.XMLBinding;
import be.nabu.libs.types.java.BeanInstance;
import be.nabu.libs.types.map.MapContent;
import be.nabu.libs.types.map.MapContentWrapper;
import be.nabu.libs.types.map.MapTypeGenerator;
import be.nabu.libs.types.mask.MaskedContent;
import be.nabu.libs.validator.api.Validation;
import be.nabu.libs.validator.api.ValidationMessage.Severity;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.mime.api.Header;
import be.nabu.utils.mime.impl.MimeHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import be.nabu.utils.mime.impl.PlainMimeContentPart;
@Path("/mcp")
public class MCPREST {

	private static final Logger LOGGER = LoggerFactory.getLogger(MCPREST.class);

	private static class ToolResult {
		private final Object structuredContent;
		private final List<Map<String, String>> content;
		private final Map<String, Object> meta;
		private final Boolean isError;
		private final String message;

		private ToolResult(Object structuredContent, List<Map<String, String>> content, Map<String, Object> meta) {
			this(structuredContent, content, meta, null, null);
		}

		private ToolResult(Object structuredContent, List<Map<String, String>> content, Map<String, Object> meta, Boolean isError, String message) {
			this.structuredContent = structuredContent;
			this.content = content;
			this.meta = meta;
			this.isError = isError;
			this.message = message;
		}
	}

	private static class EditArtifactResult {
		private final Map<String, Object> structuredContent;
		private final String resourceUri;
		private final Boolean isError;
		private final String message;

		private EditArtifactResult(Map<String, Object> structuredContent, String resourceUri) {
			this(structuredContent, resourceUri, null, null);
		}

		private EditArtifactResult(Map<String, Object> structuredContent, String resourceUri, Boolean isError, String message) {
			this.structuredContent = structuredContent;
			this.resourceUri = resourceUri;
			this.isError = isError;
			this.message = message;
		}
	}

	private static class DiffLine {
		private final char prefix;
		private final String line;
		private final int beforeLine;
		private final int afterLine;

		private DiffLine(char prefix, String line, int beforeLine, int afterLine) {
			this.prefix = prefix;
			this.line = line;
			this.beforeLine = beforeLine;
			this.afterLine = afterLine;
		}
	}

	private static final String MCP_VERSION = "2025-03-26";
	private static final String SEARCH_TOOL_NAME = "search_nabu_artifact_fragments";
	private static final String FIND_TOOL_NAME = "find_nabu_artifact_fragment";
	private static final String READ_TOOL_NAME = "read_nabu_artifact_fragment";
	private static final String READ_MULTIPLE_TOOL_NAME = "read_multiple_nabu_artifact_fragments";
	private static final String EDIT_TOOL_NAME = "edit_nabu_artifact_fragment";
	private static final String WRITE_TOOL_NAME = "write_nabu_artifact_fragment";
	private static final String CREATE_FRAGMENT_TOOL_NAME = "create_nabu_artifact_fragment";
	private static final String DELETE_FRAGMENT_TOOL_NAME = "delete_nabu_artifact_fragment";
	private static final String SEARCH_DOCUMENTATION_TOOL_NAME = "search_nabu_documentation";
	private static final String FIND_DOCUMENTATION_TOOL_NAME = "find_nabu_documentation";
	private static final String READ_DOCUMENTATION_TOOL_NAME = "read_nabu_documentation";
	private static final String READ_MULTIPLE_DOCUMENTATION_TOOL_NAME = "read_multiple_nabu_documentation";
	private static final String EDIT_DOCUMENTATION_TOOL_NAME = "edit_nabu_documentation";
	private static final String WRITE_DOCUMENTATION_TOOL_NAME = "write_nabu_documentation";
	private static final String DELETE_DOCUMENTATION_TOOL_NAME = "delete_nabu_documentation";
	private static final String CREATE_TOOL_NAME = "create_nabu_artifact";
	private static final String DELETE_TOOL_NAME = "delete_nabu_artifact";
	private static final String MOVE_TOOL_NAME = "move_nabu_artifact";
	private static final String CREATE_PROJECT_TOOL_NAME = "create_nabu_project";
	private static final String SKILLS_TOOL_NAME = "get_nabu_skills";
	private static final String INVOKE_TOOL_NAME = "invoke_nabu_service";
	private static final String TRACE_SEARCH_TOOL_NAME = "search_nabu_service_trace";
	private static final String MCP_SESSION_ID = "MCP-Session-Id";
	private static final String SESSION_MAP = "mcp.rest.sessions";
	private static final String REVIEW_RESOURCE_MAP = "mcp.rest.review.resources";
	private static final String REVIEW_RESOURCE_URI = "ui://nabu/review/diff.html";
	private static final int MAX_RESULT_BYTES = 51200;
	private static final int MAX_READ_BYTES = 50 * 1024;
	private static final int MAX_READ_LINE_BYTES = 25 * 1024;
	private static final int MAX_MULTI_READ_FRAGMENTS = 20;
	private static final int MAX_MULTI_READ_BYTES = 64000;
	private static final long SESSION_TIMEOUT = 24L * 60L * 60L * 1000L;
	private static final DateTimeFormatter TRACE_TIME_FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneOffset.UTC);
	private static final String TRACE_INCLUDE_LINKS = "mcp.trace.include_links";
	private static final ConcurrentMap<Class<?>, be.nabu.eai.repository.api.MCPTraceProvider> TRACE_PROVIDER_CACHE = new ConcurrentHashMap<Class<?>, be.nabu.eai.repository.api.MCPTraceProvider>();
	private static final String[] BUILT_IN_TOOL_NAMES = new String[] {
		SEARCH_TOOL_NAME,
		FIND_TOOL_NAME,
		READ_TOOL_NAME,
		READ_MULTIPLE_TOOL_NAME,
		EDIT_TOOL_NAME,
		WRITE_TOOL_NAME,
		CREATE_FRAGMENT_TOOL_NAME,
		DELETE_FRAGMENT_TOOL_NAME,
		SEARCH_DOCUMENTATION_TOOL_NAME,
		FIND_DOCUMENTATION_TOOL_NAME,
		READ_DOCUMENTATION_TOOL_NAME,
		READ_MULTIPLE_DOCUMENTATION_TOOL_NAME,
		EDIT_DOCUMENTATION_TOOL_NAME,
		WRITE_DOCUMENTATION_TOOL_NAME,
		DELETE_DOCUMENTATION_TOOL_NAME,
		CREATE_TOOL_NAME,
		DELETE_TOOL_NAME,
		MOVE_TOOL_NAME,
		CREATE_PROJECT_TOOL_NAME,
		SKILLS_TOOL_NAME,
		INVOKE_TOOL_NAME,
		TRACE_SEARCH_TOOL_NAME
	};

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
			ensureStaticReviewResource();
			Map<String, Object> result = new LinkedHashMap<String, Object>();
			result.put("protocolVersion", MCP_VERSION);
			Map<String, Object> capabilities = new LinkedHashMap<String, Object>();
			Map<String, Object> experimental = new LinkedHashMap<String, Object>();
			Map<String, Object> policy = new LinkedHashMap<String, Object>();
			policy.put("enabled", true);
			experimental.put("policy", policy);
			capabilities.put("experimental", experimental);
			Map<String, Object> resources = new LinkedHashMap<String, Object>();
			resources.put("list", true);
			resources.put("read", true);
			capabilities.put("resources", resources);
			Map<String, Object> tools = new LinkedHashMap<String, Object>();
			tools.put("list", true);
			tools.put("call", true);
			capabilities.put("tools", tools);
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
			MCPConfiguration configuration = resolveSessionConfiguration(request, false);
			Map<String, Object> result = new LinkedHashMap<String, Object>();
			List<Map<String, Object>> tools = new ArrayList<Map<String, Object>>();
			Map<String, Object> searchTool = new LinkedHashMap<String, Object>();
			searchTool.put("name", SEARCH_TOOL_NAME);
			searchTool.put("title", "Search nabu artifacts");
			searchTool.put("description", "Search artifact fragments ripgrep style. Match snippets include line numbers. Namespace filters artifacts by id prefix, while glob only filters fragment paths. Returned fragments are not normal files and may only be manipulated with the nabu artifact fragment tools, not standard file tools.");
			Map<String, Object> searchAnnotations = new LinkedHashMap<String, Object>();
			searchAnnotations.put("scopes", Arrays.asList("read:nabu:artifact"));
			searchAnnotations.put("intentTemplate", "Search for {pattern} [in namespaces {namespace}] [with glob {glob}] [context {context}] [before {beforeContext}] [after {afterContext}]");
			searchTool.put("annotations", searchAnnotations);
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
			Map<String, Object> searchArtifactTypes = schema("array");
			Map<String, Object> searchArtifactTypeItem = schema("string");
			searchArtifactTypeItem.put("enum", listAvailableArtifactTypes());
			searchArtifactTypes.put("items", searchArtifactTypeItem);
			searchArtifactTypes.put("description", "Optional artifact type filters. Do not pass this when 'artifactCategory' is filled in.");
			searchProperties.put("artifactType", searchArtifactTypes);
			Map<String, Object> searchArtifactCategories = schema("array");
			Map<String, Object> searchArtifactCategoryItem = schema("string");
			searchArtifactCategoryItem.put("enum", listAvailableArtifactCategories());
			searchArtifactCategories.put("items", searchArtifactCategoryItem);
			searchArtifactCategories.put("description", "Optional artifact category filters.");
			searchProperties.put("artifactCategory", searchArtifactCategories);
			searchProperties.put("caseSensitive", propertySchema("boolean", "Whether matching is case-sensitive. Defaults to false when omitted."));
			searchProperties.put("beforeContext", schema("integer"));
			searchProperties.put("afterContext", schema("integer"));
			searchProperties.put("context", schema("integer"));
			searchProperties.put("limit", propertySchema("integer", "Maximum number of results to return (>0). When omitted, all matches are considered."));
			searchProperties.put("offset", propertySchema("integer", "Number of matching results to skip before returning results. Default: 0."));
			searchInputSchema.put("properties", searchProperties);
			searchInputSchema.put("required", Arrays.asList("pattern"));
			searchTool.put("inputSchema", searchInputSchema);
			searchTool.put("outputSchema", searchOutputSchema());
			tools.add(searchTool);
			Map<String, Object> findTool = new LinkedHashMap<String, Object>();
			findTool.put("name", FIND_TOOL_NAME);
			findTool.put("title", "Find nabu artifact fragments");
			findTool.put("description", "Find indexed nabu artifact fragments using path and artifact filters. The optional pattern is a full-match regex by default, not a contains search. For substring matching use .*text.* or set glob=true and use *text*. Returned fragments are not normal files and may only be manipulated with the nabu artifact fragment tools, not standard file tools.");
			Map<String, Object> findAnnotations = new LinkedHashMap<String, Object>();
			findAnnotations.put("scopes", Arrays.asList("read:nabu:artifact"));
			findAnnotations.put("intentTemplate", "Find artifact fragments [matching full regex/glob {pattern}] [in artifact {artifactId}] [limit {limit}]");
			findTool.put("annotations", findAnnotations);
			Map<String, Object> findInputSchema = new LinkedHashMap<String, Object>();
			findInputSchema.put("type", "object");
			Map<String, Object> findProperties = new LinkedHashMap<String, Object>();
			findProperties.put("pattern", propertySchema("string", "Optional full-match regex applied to fragment paths or artifact ids, not a contains search. To match a substring use .*text.*. When glob=true, use glob syntax such as *text*."));
			findProperties.put("artifactId", propertySchema("string", "Optional artifact id filter."));
			Map<String, Object> findNamespace = propertySchema("array", "Optional artifact namespace filters. Matches the exact namespace and all descendant artifact ids. Configured and policy namespaces are applied first; this argument can only narrow further.");
			findNamespace.put("items", schema("string"));
			findProperties.put("namespace", findNamespace);
			Map<String, Object> findArtifactType = propertySchema("array", "Optional artifact type filters. Do not pass this when 'artifactId' is provided or when 'artifactCategory' is filled in.");
			Map<String, Object> findArtifactTypeItem = schema("string");
			findArtifactTypeItem.put("enum", listAvailableArtifactTypes());
			findArtifactType.put("items", findArtifactTypeItem);
			findProperties.put("artifactType", findArtifactType);
			Map<String, Object> findArtifactCategory = propertySchema("array", "Optional artifact category filters. Do not pass this when 'artifactId' is provided.");
			Map<String, Object> findArtifactCategoryItem = schema("string");
			findArtifactCategoryItem.put("enum", listAvailableArtifactCategories());
			findArtifactCategory.put("items", findArtifactCategoryItem);
			findProperties.put("artifactCategory", findArtifactCategory);
			findProperties.put("glob", propertySchema("boolean", "If true, interpret pattern as a full-match glob instead of a regex. For substring matching with glob use *text*."));
			findProperties.put("limit", propertySchema("integer", "Maximum number of results to return (>0)."));
			findProperties.put("offset", propertySchema("integer", "Number of matching results to skip before returning results."));
			findProperties.put("caseSensitive", propertySchema("boolean", "Whether matching is case-sensitive. Defaults to false when omitted."));
			findInputSchema.put("properties", findProperties);
			findTool.put("inputSchema", findInputSchema);
			findTool.put("outputSchema", findOutputSchema());
			tools.add(findTool);
			Map<String, Object> readTool = new LinkedHashMap<String, Object>();
			readTool.put("name", READ_TOOL_NAME);
			readTool.put("title", "Read nabu artifact fragment");
			readTool.put("description", "Read lines from an indexed nabu artifact fragment. Returned content is raw text.");
			Map<String, Object> readAnnotations = new LinkedHashMap<String, Object>();
			readAnnotations.put("scopes", Arrays.asList("read:nabu:artifact"));
			readAnnotations.put("intentTemplate", "Read artifact {artifactId} fragment {path} [from line {startLine}] [limit {limit}]");
			readTool.put("annotations", readAnnotations);
			Map<String, Object> readInputSchema = new LinkedHashMap<String, Object>();
			readInputSchema.put("type", "object");
			Map<String, Object> readProperties = new LinkedHashMap<String, Object>();
			readProperties.put("artifactId", propertySchema("string", "Artifact id containing the fragment."));
			readProperties.put("path", propertySchema("string", "Path to the fragment inside the artifact."));
			readProperties.put("startLine", propertySchema("integer", "1-based line number to start reading from. Default: 1."));
			readProperties.put("limit", propertySchema("integer", "Maximum number of lines to return (>0). Default: 200."));
			readInputSchema.put("properties", readProperties);
			readInputSchema.put("required", Arrays.asList("artifactId", "path"));
			readTool.put("inputSchema", readInputSchema);
			readTool.put("outputSchema", readOutputSchema());
			tools.add(readTool);
			Map<String, Object> readMultipleTool = new LinkedHashMap<String, Object>();
			readMultipleTool.put("name", READ_MULTIPLE_TOOL_NAME);
			readMultipleTool.put("title", "Read multiple nabu artifact fragments");
			readMultipleTool.put("description", "Read lines from multiple indexed nabu artifact fragments in one call. Returned content is raw text. Results may be truncated to avoid excessive output.");
			Map<String, Object> readMultipleAnnotations = new LinkedHashMap<String, Object>();
			readMultipleAnnotations.put("scopes", Arrays.asList("read:nabu:artifact"));
			readMultipleAnnotations.put("intentTemplate", "Read multiple artifact fragments");
			readMultipleTool.put("annotations", readMultipleAnnotations);
			Map<String, Object> readMultipleInputSchema = new LinkedHashMap<String, Object>();
			readMultipleInputSchema.put("type", "object");
			Map<String, Object> readMultipleProperties = new LinkedHashMap<String, Object>();
			Map<String, Object> fragments = schema("array");
			fragments.put("description", "Fragments to read. Keep this list small to avoid large responses.");
			Map<String, Object> fragmentItem = new LinkedHashMap<String, Object>();
			fragmentItem.put("type", "object");
			Map<String, Object> fragmentProperties = new LinkedHashMap<String, Object>();
			fragmentProperties.put("artifactId", propertySchema("string", "Artifact id containing the fragment."));
			fragmentProperties.put("path", propertySchema("string", "Path to the fragment inside the artifact."));
			fragmentProperties.put("startLine", propertySchema("integer", "1-based line number to start reading from. Default: 1."));
			fragmentProperties.put("limit", propertySchema("integer", "Maximum number of lines to return (>0). Default: 200."));
			fragmentItem.put("properties", fragmentProperties);
			fragmentItem.put("required", Arrays.asList("artifactId", "path"));
			fragments.put("items", fragmentItem);
			readMultipleProperties.put("fragments", fragments);
			readMultipleInputSchema.put("properties", readMultipleProperties);
			readMultipleInputSchema.put("required", Arrays.asList("fragments"));
			readMultipleTool.put("inputSchema", readMultipleInputSchema);
			readMultipleTool.put("outputSchema", readMultipleOutputSchema());
			tools.add(readMultipleTool);
			Map<String, Object> editTool = new LinkedHashMap<String, Object>();
			editTool.put("name", EDIT_TOOL_NAME);
			editTool.put("title", "Edit nabu artifact fragment");
			editTool.put("description", "Replace exact matches in an editable artifact fragment. Always use leading tabs instead of leading spaces when editing indentation-sensitive content.");
			Map<String, Object> editAnnotations = new LinkedHashMap<String, Object>();
			editAnnotations.put("scopes", Arrays.asList("write:nabu:artifact"));
			editAnnotations.put("preview", true);
			editAnnotations.put("intentTemplate", "Edit artifact {artifactId} fragment {path}");
			editTool.put("annotations", editAnnotations);
			Map<String, Object> editInputSchema = new LinkedHashMap<String, Object>();
			editInputSchema.put("type", "object");
			Map<String, Object> editProperties = new LinkedHashMap<String, Object>();
			editProperties.put("artifactId", propertySchema("string", "Artifact id containing the fragment."));
			editProperties.put("path", propertySchema("string", "Path to the fragment inside the artifact."));
			editProperties.put("edits", editSchema());
			editInputSchema.put("properties", editProperties);
			editInputSchema.put("required", Arrays.asList("artifactId", "path", "edits"));
			editTool.put("inputSchema", editInputSchema);
			editTool.put("outputSchema", editOutputSchema());
			editTool.put("_meta", buildToolDefinitionMeta(REVIEW_RESOURCE_URI));
			tools.add(editTool);
			Map<String, Object> writeTool = new LinkedHashMap<String, Object>();
			writeTool.put("name", WRITE_TOOL_NAME);
			writeTool.put("title", "Write nabu artifact fragment");
			writeTool.put("description", "Use this tool to overwrite, append, or prepend a whole editable artifact fragment. Always use leading tabs instead of leading spaces when writing indentation-sensitive content.");
			Map<String, Object> writeAnnotations = new LinkedHashMap<String, Object>();
			writeAnnotations.put("scopes", Arrays.asList("write:nabu:artifact"));
			writeAnnotations.put("preview", true);
			writeAnnotations.put("intentTemplate", "Write artifact {artifactId} fragment {path} [mode {mode}]");
			writeTool.put("annotations", writeAnnotations);
			Map<String, Object> writeInputSchema = new LinkedHashMap<String, Object>();
			writeInputSchema.put("type", "object");
			Map<String, Object> writeProperties = new LinkedHashMap<String, Object>();
			writeProperties.put("artifactId", propertySchema("string", "Artifact id containing the fragment."));
			writeProperties.put("path", propertySchema("string", "Path to the fragment inside the artifact."));
			writeProperties.put("content", propertySchema("string", "New fragment content to write or preview."));
			Map<String, Object> mode = propertySchema("string", "Write mode. Default: overwrite.");
			mode.put("enum", Arrays.asList("overwrite", "append", "prepend"));
			writeProperties.put("mode", mode);
			writeInputSchema.put("properties", writeProperties);
			writeInputSchema.put("required", Arrays.asList("artifactId", "path", "content"));
			writeTool.put("inputSchema", writeInputSchema);
			writeTool.put("outputSchema", writeOutputSchema());
			writeTool.put("_meta", buildToolDefinitionMeta(REVIEW_RESOURCE_URI));
			tools.add(writeTool);
			addDocumentationTools(tools);
			List<String> dynamicArtifactTypes = listDynamicArtifactTypes();
			if (!dynamicArtifactTypes.isEmpty()) {
				Map<String, Object> createFragmentTool = new LinkedHashMap<String, Object>();
				createFragmentTool.put("name", CREATE_FRAGMENT_TOOL_NAME);
				createFragmentTool.put("title", "Create nabu artifact fragment");
				createFragmentTool.put("description", buildCreateFragmentToolDescription(dynamicArtifactTypes));
				Map<String, Object> createFragmentAnnotations = new LinkedHashMap<String, Object>();
				createFragmentAnnotations.put("scopes", Arrays.asList("write:nabu:artifact"));
				createFragmentAnnotations.put("intentTemplate", "Create artifact fragment {path} in {artifactId}");
				createFragmentTool.put("annotations", createFragmentAnnotations);
				Map<String, Object> createFragmentInputSchema = new LinkedHashMap<String, Object>();
				createFragmentInputSchema.put("type", "object");
				Map<String, Object> createFragmentProperties = new LinkedHashMap<String, Object>();
				createFragmentProperties.put("artifactId", propertySchema("string", "Artifact id containing the fragment."));
				createFragmentProperties.put("path", propertySchema("string", "Path to create inside the artifact."));
				createFragmentProperties.put("content", propertySchema("string", "Optional initial fragment content. Defaults to empty content when omitted."));
				createFragmentInputSchema.put("properties", createFragmentProperties);
				createFragmentInputSchema.put("required", Arrays.asList("artifactId", "path"));
				createFragmentTool.put("inputSchema", createFragmentInputSchema);
				createFragmentTool.put("outputSchema", writeOutputSchema());
				createFragmentTool.put("_meta", buildToolDefinitionMeta(REVIEW_RESOURCE_URI));
				tools.add(createFragmentTool);
				Map<String, Object> deleteFragmentTool = new LinkedHashMap<String, Object>();
				deleteFragmentTool.put("name", DELETE_FRAGMENT_TOOL_NAME);
				deleteFragmentTool.put("title", "Delete nabu artifact fragment");
				deleteFragmentTool.put("description", buildDeleteFragmentToolDescription(dynamicArtifactTypes));
				Map<String, Object> deleteFragmentAnnotations = new LinkedHashMap<String, Object>();
				deleteFragmentAnnotations.put("scopes", Arrays.asList("write:nabu:artifact"));
				deleteFragmentAnnotations.put("intentTemplate", "Delete artifact fragment {path} from {artifactId}");
				deleteFragmentTool.put("annotations", deleteFragmentAnnotations);
				Map<String, Object> deleteFragmentInputSchema = new LinkedHashMap<String, Object>();
				deleteFragmentInputSchema.put("type", "object");
				Map<String, Object> deleteFragmentProperties = new LinkedHashMap<String, Object>();
				deleteFragmentProperties.put("artifactId", propertySchema("string", "Artifact id containing the fragment."));
				deleteFragmentProperties.put("path", propertySchema("string", "Path to delete inside the artifact."));
				deleteFragmentInputSchema.put("properties", deleteFragmentProperties);
				deleteFragmentInputSchema.put("required", Arrays.asList("artifactId", "path"));
				deleteFragmentTool.put("inputSchema", deleteFragmentInputSchema);
				deleteFragmentTool.put("outputSchema", writeOutputSchema());
				deleteFragmentTool.put("_meta", buildToolDefinitionMeta(REVIEW_RESOURCE_URI));
				tools.add(deleteFragmentTool);
			}
			List<String> creatableArtifactTypes = listCreatableArtifactTypes();
			if (!creatableArtifactTypes.isEmpty()) {
				Map<String, Object> createTool = new LinkedHashMap<String, Object>();
				createTool.put("name", CREATE_TOOL_NAME);
				createTool.put("title", "Create nabu artifact");
				createTool.put("description", "Create a new nabu artifact in the given namespace using a creatable artifact fragment manager. Always read the `design:repository` skill before creating a new artifact for the first time.");
				Map<String, Object> createAnnotations = new LinkedHashMap<String, Object>();
				createAnnotations.put("scopes", Arrays.asList("write:nabu:artifact"));
				createAnnotations.put("intentTemplate", "Creating {type}: {namespace}.{name}");
				createTool.put("annotations", createAnnotations);
				Map<String, Object> createInputSchema = new LinkedHashMap<String, Object>();
				createInputSchema.put("type", "object");
				Map<String, Object> createProperties = new LinkedHashMap<String, Object>();
				createProperties.put("namespace", propertySchema("string", "Namespace where the new artifact should be created."));
				createProperties.put("name", propertySchema("string", "Name of the new artifact."));
				Map<String, Object> createType = propertySchema("string", "Artifact type to create.");
				createType.put("enum", creatableArtifactTypes);
				createProperties.put("type", createType);
				createInputSchema.put("properties", createProperties);
				createInputSchema.put("required", Arrays.asList("namespace", "name", "type"));
				tools.add(createTool);
				createTool.put("inputSchema", createInputSchema);
			}
			Map<String, Object> deleteTool = new LinkedHashMap<String, Object>();
			deleteTool.put("name", DELETE_TOOL_NAME);
			deleteTool.put("title", "Delete nabu artifact");
			deleteTool.put("description", "Delete an existing nabu artifact by artifact id.");
			Map<String, Object> deleteAnnotations = new LinkedHashMap<String, Object>();
			deleteAnnotations.put("scopes", Arrays.asList("write:nabu:artifact"));
			deleteAnnotations.put("intentTemplate", "Deleting {artifactId}");
			deleteTool.put("annotations", deleteAnnotations);
			Map<String, Object> deleteInputSchema = new LinkedHashMap<String, Object>();
			deleteInputSchema.put("type", "object");
			Map<String, Object> deleteProperties = new LinkedHashMap<String, Object>();
			deleteProperties.put("artifactId", propertySchema("string", "Artifact id to delete."));
			deleteInputSchema.put("properties", deleteProperties);
			deleteInputSchema.put("required", Arrays.asList("artifactId"));
			deleteTool.put("inputSchema", deleteInputSchema);
			tools.add(deleteTool);
			Map<String, Object> moveTool = new LinkedHashMap<String, Object>();
			moveTool.put("name", MOVE_TOOL_NAME);
			moveTool.put("title", "Move nabu artifact");
			moveTool.put("description", "Move or rename an existing nabu artifact or namespace to a new artifact id or namespace. This uses the repository move flow, including dependency relinking where supported.");
			Map<String, Object> moveAnnotations = new LinkedHashMap<String, Object>();
			moveAnnotations.put("scopes", Arrays.asList("write:nabu:artifact"));
			moveAnnotations.put("intentTemplate", "Moving {oldId} to {newId}");
			moveTool.put("annotations", moveAnnotations);
			Map<String, Object> moveInputSchema = new LinkedHashMap<String, Object>();
			moveInputSchema.put("type", "object");
			Map<String, Object> moveProperties = new LinkedHashMap<String, Object>();
			moveProperties.put("oldId", propertySchema("string", "Current artifact id or namespace to move."));
			moveProperties.put("newId", propertySchema("string", "Destination artifact id or namespace after the move."));
			moveInputSchema.put("properties", moveProperties);
			moveInputSchema.put("required", Arrays.asList("oldId", "newId"));
			moveTool.put("inputSchema", moveInputSchema);
			tools.add(moveTool);
			Map<String, Object> createProjectTool = new LinkedHashMap<String, Object>();
			createProjectTool.put("name", CREATE_PROJECT_TOOL_NAME);
			createProjectTool.put("title", "Create nabu project");
			createProjectTool.put("description", "Create a new nabu project at the repository root.");
			Map<String, Object> createProjectAnnotations = new LinkedHashMap<String, Object>();
			createProjectAnnotations.put("scopes", Arrays.asList("write:nabu:project"));
			createProjectAnnotations.put("intentTemplate", "Creating project {name} [{type}]");
			createProjectTool.put("annotations", createProjectAnnotations);
			Map<String, Object> createProjectInputSchema = new LinkedHashMap<String, Object>();
			createProjectInputSchema.put("type", "object");
			Map<String, Object> createProjectProperties = new LinkedHashMap<String, Object>();
			createProjectProperties.put("name", propertySchema("string", "Name of the new project."));
			Map<String, Object> createProjectType = propertySchema("string", "Project type to create.");
			createProjectType.put("enum", Arrays.asList("utility", "integration", "business", "application", "testing"));
			createProjectProperties.put("type", createProjectType);
			createProjectInputSchema.put("properties", createProjectProperties);
			createProjectInputSchema.put("required", Arrays.asList("name", "type"));
			createProjectTool.put("inputSchema", createProjectInputSchema);
			tools.add(createProjectTool);
			Map<String, Object> skillsTool = new LinkedHashMap<String, Object>();
			skillsTool.put("name", SKILLS_TOOL_NAME);
			skillsTool.put("title", "Get nabu skills");
			skillsTool.put("description", "Fetch guidance for known nabu skills. Always fetch the relevant skill before using a tool for the first time when such a skill exists. Always fetch the relevant skill before editing an artifact for the first time if there is a skill for it.");
			Map<String, Object> skillsAnnotations = new LinkedHashMap<String, Object>();
			skillsAnnotations.put("scopes", Arrays.asList("read:nabu:artifact"));
			skillsTool.put("annotations", skillsAnnotations);
			Map<String, Object> skillsInputSchema = new LinkedHashMap<String, Object>();
			skillsInputSchema.put("type", "object");
			Map<String, Object> skillsProperties = new LinkedHashMap<String, Object>();
			Map<String, Object> skills = schema("array");
			Map<String, Object> skillItem = schema("string");
			skillItem.put("enum", listAvailableSkillNames());
			skills.put("items", skillItem);
			skills.put("description", "Skill names to fetch, for example artifact:structure or tool:invoke_nabu_service.");
			skillsProperties.put("skills", skills);
			skillsInputSchema.put("properties", skillsProperties);
			skillsInputSchema.put("required", Arrays.asList("skills"));
			skillsTool.put("inputSchema", skillsInputSchema);
			tools.add(skillsTool);
			Map<String, Object> invokeTool = new LinkedHashMap<String, Object>();
			invokeTool.put("name", INVOKE_TOOL_NAME);
			invokeTool.put("title", "Invoke nabu service");
			invokeTool.put("description", "Invoke a nabu service through the same execution path as ServerREST, with optional one-off trace capture.");
			Map<String, Object> invokeAnnotations = new LinkedHashMap<String, Object>();
			invokeAnnotations.put("scopes", Arrays.asList("execute:nabu:service"));
			invokeAnnotations.put("intentTemplate", "Invoking {serviceId}");
			invokeAnnotations.put("inputTemplate", "Service: {serviceId}\nInput:\n```json\n{input}\n```");
			invokeAnnotations.put("outputTemplate", "Output:\n```json\n{output}\n```[\nTrace: {traceId}]");
			invokeTool.put("annotations", invokeAnnotations);
			Map<String, Object> invokeInputSchema = new LinkedHashMap<String, Object>();
			invokeInputSchema.put("type", "object");
			Map<String, Object> invokeProperties = new LinkedHashMap<String, Object>();
			invokeProperties.put("serviceId", propertySchema("string", "Service id to invoke."));
			invokeProperties.put("runAs", propertySchema("string", "Optional user alias to impersonate for this invoke."));
			invokeProperties.put("runAsRealm", propertySchema("string", "Optional realm for runAs."));
			Map<String, Object> features = schema("array");
			features.put("items", schema("string"));
			features.put("description", "Optional enabled features for the execution context.");
			invokeProperties.put("features", features);
			invokeProperties.put("serviceContext", propertySchema("string", "Optional service context header value."));
			invokeProperties.put("trace", propertySchema("boolean", "If true, stream a trace xml file for this invoke. Default: false."));
			Map<String, Object> input = new LinkedHashMap<String, Object>();
			input.put("type", "object");
			input.put("description", "JSON object payload for the service input. Property names and nested values may vary per service, but this field itself must be a valid JSON object, not free-form text or another format.");
			input.put("additionalProperties", true);
			invokeProperties.put("input", input);
			invokeInputSchema.put("properties", invokeProperties);
			invokeInputSchema.put("required", Arrays.asList("serviceId"));
			invokeTool.put("inputSchema", invokeInputSchema);
			invokeTool.put("outputSchema", invokeOutputSchema());
			tools.add(invokeTool);
			Map<String, Object> traceSearchTool = new LinkedHashMap<String, Object>();
			traceSearchTool.put("name", TRACE_SEARCH_TOOL_NAME);
			traceSearchTool.put("title", "Search nabu service trace");
			traceSearchTool.put("description", "Run XPath queries against a streamed nabu service trace xml file and return stringified xml results.");
			Map<String, Object> traceSearchAnnotations = new LinkedHashMap<String, Object>();
			traceSearchAnnotations.put("scopes", Arrays.asList("read:nabu:artifact"));
			traceSearchTool.put("annotations", traceSearchAnnotations);
			Map<String, Object> traceSearchInputSchema = new LinkedHashMap<String, Object>();
			traceSearchInputSchema.put("type", "object");
			Map<String, Object> traceSearchProperties = new LinkedHashMap<String, Object>();
			traceSearchProperties.put("traceId", propertySchema("string", "Trace id to search."));
			Map<String, Object> queries = schema("array");
			queries.put("items", schema("string"));
			queries.put("description", "XPath expressions to evaluate against the trace xml.");
			traceSearchProperties.put("queries", queries);
			traceSearchProperties.put("depth", propertySchema("integer", "Nested invoke depth to include in matched results. Default: 1."));
			traceSearchProperties.put("limit", propertySchema("integer", "Maximum number of xpath hits to return per query. Default: 20."));
			traceSearchProperties.put("offset", propertySchema("integer", "Number of xpath hits to skip per query. Default: 0."));
			traceSearchInputSchema.put("properties", traceSearchProperties);
			traceSearchInputSchema.put("required", Arrays.asList("traceId", "queries"));
			traceSearchTool.put("inputSchema", traceSearchInputSchema);
			traceSearchTool.put("outputSchema", traceSearchOutputSchema());
			tools.add(traceSearchTool);
			addCustomTools(tools, configuration);
			result.put("tools", tools);
			response.put("result", result);
			return json(response, null);
		}
		if ("tools/call".equals(method)) {
			MCPConfiguration configuration = resolveSessionConfiguration(request, true);
			Map<String, Object> params = map(rpc.get("params"));
			String name = params == null ? null : string(params.get("name"));
			MCPToolProvider<?> customToolProvider = isBuiltInToolName(name) ? null : findCustomToolProvider(name, configuration);
			if (!isBuiltInToolName(name) && customToolProvider == null) {
				response.put("error", error(-32602, "Unknown tool: " + name));
				return json(response, null);
			}
			Map<String, Object> arguments = map(params.get("arguments"));
			Map<String, Object> meta = map(params.get("_meta"));
			Map<String, Object> result = new LinkedHashMap<String, Object>();
			ToolResult toolResult;
			try {
				if (customToolProvider != null) {
					boolean preview = asBoolean(meta == null ? null : meta.get("preview"));
					toolResult = customToolResult(customToolProvider, arguments, request, configuration, meta, preview);
				}
				else if (SEARCH_TOOL_NAME.equals(name)) {
					toolResult = searchToolResult(arguments, meta, configuration);
				}
				else if (FIND_TOOL_NAME.equals(name)) {
					toolResult = findToolResult(arguments, meta, configuration);
				}
				else if (READ_TOOL_NAME.equals(name)) {
					toolResult = readToolResult(arguments, meta, configuration);
				}
				else if (READ_MULTIPLE_TOOL_NAME.equals(name)) {
					toolResult = readMultipleToolResult(arguments, meta, configuration);
				}
				else if (SEARCH_DOCUMENTATION_TOOL_NAME.equals(name)) {
					toolResult = searchDocumentationToolResult(arguments, meta, configuration);
				}
				else if (FIND_DOCUMENTATION_TOOL_NAME.equals(name)) {
					toolResult = findDocumentationToolResult(arguments, meta, configuration);
				}
				else if (READ_DOCUMENTATION_TOOL_NAME.equals(name)) {
					toolResult = readDocumentationToolResult(arguments, meta, configuration);
				}
				else if (READ_MULTIPLE_DOCUMENTATION_TOOL_NAME.equals(name)) {
					toolResult = readMultipleDocumentationToolResult(arguments, meta, configuration);
				}
				else if (DELETE_DOCUMENTATION_TOOL_NAME.equals(name)) {
					boolean preview = asBoolean(meta == null ? null : meta.get("preview"));
					toolResult = deleteDocumentationToolResult(arguments, meta, configuration, preview);
				}
				else if (EDIT_DOCUMENTATION_TOOL_NAME.equals(name) || WRITE_DOCUMENTATION_TOOL_NAME.equals(name)) {
					boolean preview = asBoolean(meta == null ? null : meta.get("preview"));
					toolResult = EDIT_DOCUMENTATION_TOOL_NAME.equals(name)
						? editDocumentationToolResult(arguments, meta, configuration, preview)
						: writeDocumentationToolResult(arguments, meta, configuration, preview);
				}
				else if (CREATE_FRAGMENT_TOOL_NAME.equals(name)) {
					toolResult = createFragmentToolResult(arguments, meta, configuration);
				}
				else if (DELETE_FRAGMENT_TOOL_NAME.equals(name)) {
					toolResult = deleteFragmentToolResult(arguments, meta, configuration);
				}
				else if (CREATE_TOOL_NAME.equals(name)) {
					toolResult = createToolResult(arguments, meta, configuration);
				}
				else if (DELETE_TOOL_NAME.equals(name)) {
					toolResult = deleteToolResult(arguments, meta, configuration);
				}
				else if (MOVE_TOOL_NAME.equals(name)) {
					toolResult = moveToolResult(arguments, meta, configuration);
				}
				else if (CREATE_PROJECT_TOOL_NAME.equals(name)) {
					toolResult = createProjectToolResult(arguments);
				}
				else if (SKILLS_TOOL_NAME.equals(name)) {
					toolResult = skillsToolResult(arguments);
				}
				else if (INVOKE_TOOL_NAME.equals(name)) {
					toolResult = invokeToolResult(arguments);
				}
				else if (TRACE_SEARCH_TOOL_NAME.equals(name)) {
					toolResult = traceSearchToolResult(arguments);
				}
				else {
					boolean preview = asBoolean(meta == null ? null : meta.get("preview"));
					toolResult = EDIT_TOOL_NAME.equals(name)
						? editToolResult(arguments, meta, configuration, preview)
						: writeToolResult(arguments, meta, configuration, preview);
				}
			}
			catch (Exception e) {
				toolResult = errorToolResult(name, e);
			}
			result.put("content", toolResult.content);
			result.put("structuredContent", toolResult.structuredContent);
			if (toolResult.isError != null) {
				result.put("isError", toolResult.isError);
			}
			if (toolResult.message != null) {
				result.put("message", toolResult.message);
			}
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
		Integer limit = input != null && input.getLimit() != null && input.getLimit().intValue() > 0 ? input.getLimit() : null;
		int offset = input != null && input.getOffset() != null && input.getOffset().intValue() >= 0 ? input.getOffset().intValue() : 0;
		List<MCPFragmentSearchResult> allResults = search(input, meta, configuration);
		int from = Math.min(offset, allResults.size());
		int to = limit == null ? allResults.size() : Math.min(from + limit.intValue(), allResults.size());
		List<MCPFragmentSearchResult> page = new ArrayList<MCPFragmentSearchResult>(allResults.subList(from, to));
		boolean truncated = to < allResults.size();
		Map<String, Object> structuredContent = optimizeResults(input.getPattern(), page, allResults.size(), offset, limit, truncated);
		List<Map<String, String>> content = textContent(buildSummaryText(structuredContent));
		return new ToolResult(structuredContent, content, buildToolMeta(null, buildSearchDisplayMessage(structuredContent)));
	}

	private ToolResult skillsToolResult(Map<String, Object> arguments) {
		List<String> skills = stringList(arguments == null ? null : arguments.get("skills"));
		String markdown = buildSkillsMarkdown(skills);
		Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
		structuredContent.put("markdown", markdown);
		structuredContent.put("count", skills == null ? 0 : skills.size());
		structuredContent.put("skills", skills == null ? Collections.emptyList() : skills);
		return new ToolResult(structuredContent, textContent(markdown), buildToolMeta(null, buildSkillsDisplayMessage(structuredContent)));
	}

	private ToolResult invokeToolResult(Map<String, Object> arguments) throws IOException, ParseException {
		String serviceId = requiredString(arguments, "serviceId", "MISSING_SERVICE_ID");
		DefinedService service = (DefinedService) server.getRepository().resolve(serviceId);
		if (service == null) {
			throw protocolError("UNKNOWN_SERVICE", "Service not found: '" + serviceId + "'.");
		}
		Map<String, Object> input = map(arguments.get("input"));
		ExecutionContext executionContext = server.getRepository().newExecutionContext(resolvePrincipal(arguments));
		List<String> features = stringList(arguments.get("features"));
		if (features != null && !features.isEmpty() && executionContext instanceof FeaturedExecutionContext) {
			((FeaturedExecutionContext) executionContext).getEnabledFeatures().addAll(features);
		}
		String serviceContext = string(arguments.get("serviceContext"));
		String requestedRunAs = string(arguments.get("runAs"));
		String requestedRunAsRealm = string(arguments.get("runAsRealm"));
		Map<String, Object> previousGlobalContext = ServiceRuntime.getGlobalContext();
		TraceRun traceRun = null;
		Instant started = Instant.now();
		try {
			ServiceRuntime.setGlobalContext(new LinkedHashMap<String, Object>());
			ServiceRuntime.getGlobalContext().put("service.context", serviceContext == null || serviceContext.trim().isEmpty() ? serviceId : serviceContext);
			ServiceRuntime.getGlobalContext().put("service.source", "mcp.invoke");
			if (requestedRunAs != null && !requestedRunAs.trim().isEmpty()) {
				ServiceRuntime.getGlobalContext().put("mcp.requestedRunAs", requestedRunAs.trim());
				if (requestedRunAsRealm != null && !requestedRunAsRealm.trim().isEmpty()) {
					ServiceRuntime.getGlobalContext().put("mcp.requestedRunAsRealm", requestedRunAsRealm.trim());
				}
			}
			if (asBoolean(arguments.get("trace"))) {
				traceRun = TraceRun.start(server.getRepository(), service);
				if (executionContext.getServiceContext().getServiceTrackerProvider() instanceof be.nabu.eai.repository.api.ModifiableServiceRuntimeTrackerProvider) {
					((be.nabu.eai.repository.api.ModifiableServiceRuntimeTrackerProvider) executionContext.getServiceContext().getServiceTrackerProvider()).addTracker(service, traceRun.tracker, true);
				}
				else {
					throw protocolError("TRACE_UNSUPPORTED", "Tracing is not supported for one-off service invocations in the current execution context.");
				}
			}
			ComplexContent serviceInput = bindServiceInput(service, input);
			ServiceResult serviceResult = server.getRepository().getServiceRunner().run(service, executionContext, serviceInput).get();
			Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
			structuredContent.put("serviceId", serviceId);
			structuredContent.put("started", TRACE_TIME_FORMATTER.format(started));
			structuredContent.put("stopped", TRACE_TIME_FORMATTER.format(Instant.now()));
			structuredContent.put("trace", traceRun != null);
			if (traceRun != null) {
				structuredContent.put("traceId", traceRun.traceId);
			}
			if (serviceResult.getException() != null) {
				structuredContent.put("exception", stacktrace(serviceResult.getException()));
				return new ToolResult(structuredContent, textContent(toJson(structuredContent)), buildToolMeta(null, buildInvokeDisplayMessage(true, structuredContent)), true, firstExceptionMessage(serviceResult.getException()));
			}
			Object output = unwrap(serviceResult.getOutput());
			structuredContent.put("output", output);
			return new ToolResult(structuredContent, textContent(toJson(structuredContent)), buildToolMeta(null, buildInvokeDisplayMessage(false, structuredContent)), false, null);
		}
		catch (HTTPException e) {
			throw e;
		}
		catch (Exception e) {
			throw new IOException(e);
		}
		finally {
			if (traceRun != null) {
				traceRun.close();
			}
			ServiceRuntime.setGlobalContext(previousGlobalContext);
		}
	}

	private ToolResult traceSearchToolResult(Map<String, Object> arguments) throws IOException {
		String traceId = requiredString(arguments, "traceId", "MISSING_TRACE_ID");
		List<String> queries = stringList(arguments.get("queries"));
		if (queries == null || queries.isEmpty()) {
			throw protocolError("MISSING_QUERIES", "Missing required argument 'queries': provide at least one XPath query.");
		}
		int depth = Math.max(0, integer(arguments.get("depth"), 1));
		int limit = Math.max(1, integer(arguments.get("limit"), 20));
		int offset = Math.max(0, integer(arguments.get("offset"), 0));
		Map<String, Object> structuredContent = searchTrace(traceId, queries, depth, limit, offset);
		return new ToolResult(structuredContent, textContent(toJson(structuredContent)), buildToolMeta(null, buildTraceSearchDisplayMessage(structuredContent)));
	}

	private String buildSkillsMarkdown(List<String> skills) {
		if (skills == null || skills.isEmpty()) {
			throw protocolError("MISSING_SKILLS", "Missing required argument 'skills'.");
		}
		StringBuilder builder = new StringBuilder();
		for (String skill : skills) {
			builder.append("# Skill: `").append(skill).append("`\n\n");
			String guidelines = getSkillGuidelines(skill);
			if (guidelines == null || guidelines.trim().isEmpty()) {
				builder.append("No guidance is available for this skill.\n\n");
			}
			else {
				builder.append(guidelines.trim()).append("\n\n");
			}
		}
		return builder.toString().trim();
	}

	private List<String> listAvailableSkillNames() {
		Set<String> skills = new LinkedHashSet<String>();
		for (String artifactType : listArtifactSkills()) {
			skills.add("artifact:" + artifactType);
		}
		for (String toolName : listToolSkills()) {
			skills.add("tool:" + toolName);
		}
		for (String designSkill : listDesignSkills()) {
			skills.add("design:" + designSkill);
		}
		return new ArrayList<String>(skills);
	}

	private List<String> listAvailableArtifactTypes() {
		return listArtifactSkills();
	}

	private List<String> listAvailableArtifactCategories() {
		Set<String> categories = new LinkedHashSet<String>();
		for (String artifactCategory : listArtifactCategories()) {
			categories.add(artifactCategory);
		}
		return new ArrayList<String>(categories);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private List<String> listArtifactSkills() {
		Set<String> artifactTypes = new LinkedHashSet<String>();
		for (Class<ArtifactFragmentManager> managerClass : EAIRepositoryUtils.getImplementationsFor(server.getRepository().getClassLoader(), ArtifactFragmentManager.class, false)) {
			try {
				ArtifactFragmentManager manager = managerClass.newInstance();
				String guidelines = manager.getGuidelines(null);
				if (guidelines == null || guidelines.trim().isEmpty()) {
					continue;
				}
				String artifactType = artifactTypeForManager(manager);
				if (artifactType != null) {
					artifactTypes.add(artifactType);
				}
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return new ArrayList<String>(artifactTypes);
	}

	private List<String> listToolSkills() {
		return Collections.singletonList(INVOKE_TOOL_NAME);
	}

	@SuppressWarnings({ "rawtypes" })
	private List<String> listDynamicArtifactTypes() {
		Set<String> artifactTypes = new LinkedHashSet<String>();
		for (Class<ArtifactFragmentManager> managerClass : EAIRepositoryUtils.getImplementationsFor(server.getRepository().getClassLoader(), ArtifactFragmentManager.class, false)) {
			try {
				ArtifactFragmentManager manager = managerClass.newInstance();
				if (manager instanceof DynamicArtifactFragmentManager) {
					String artifactType = artifactTypeForManager(manager);
					if (artifactType != null) {
						artifactTypes.add(artifactType);
					}
				}
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return new ArrayList<String>(artifactTypes);
	}

	@SuppressWarnings({ "rawtypes" })
	private List<String> listCreatableArtifactTypes() {
		Set<String> artifactTypes = new LinkedHashSet<String>();
		for (Class<ArtifactFragmentManager> managerClass : EAIRepositoryUtils.getImplementationsFor(server.getRepository().getClassLoader(), ArtifactFragmentManager.class, false)) {
			try {
				ArtifactFragmentManager manager = managerClass.newInstance();
				if (manager instanceof CreatableArtifactFragmentManager) {
					String artifactType = artifactTypeForManager(manager);
					if (artifactType != null) {
						artifactTypes.add(artifactType);
					}
				}
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return new ArrayList<String>(artifactTypes);
	}

	private List<String> listDesignSkills() {
		return Collections.singletonList("repository");
	}

	@SuppressWarnings({ "rawtypes" })
	private List<String> listArtifactCategories() {
		Set<String> artifactCategories = new LinkedHashSet<String>();
		for (Class<ArtifactFragmentManager> managerClass : EAIRepositoryUtils.getImplementationsFor(server.getRepository().getClassLoader(), ArtifactFragmentManager.class, false)) {
			try {
				ArtifactFragmentManager manager = managerClass.newInstance();
				String artifactCategory = artifactCategoryForManager(manager);
				if (artifactCategory != null) {
					artifactCategories.add(artifactCategory);
				}
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return new ArrayList<String>(artifactCategories);
	}

	@SuppressWarnings("rawtypes")
	private String artifactTypeForManager(ArtifactFragmentManager manager) {
		String artifactType = manager.getArtifactType();
		return artifactType == null || artifactType.trim().isEmpty() ? null : artifactType.trim();
	}

	@SuppressWarnings("rawtypes")
	private String artifactCategoryForManager(ArtifactFragmentManager manager) {
		String artifactCategory = manager.getArtifactCategory();
		return artifactCategory == null || artifactCategory.trim().isEmpty() ? null : artifactCategory.trim();
	}

	private String getSkillGuidelines(String skill) {
		if (skill == null || skill.trim().isEmpty()) {
			throw protocolError("MISSING_SKILL", "Skill name must not be empty.");
		}
		if (skill.startsWith("artifact:")) {
			String artifactType = skill.substring("artifact:".length()).trim();
			ArtifactFragmentManager<?> manager = findFragmentManagerByArtifactType(artifactType);
			if (manager == null) {
				throw protocolError("UNKNOWN_SKILL", "Unknown skill: " + skill);
			}
			return manager.getGuidelines(null);
		}
		if (skill.startsWith("tool:")) {
			String toolName = skill.substring("tool:".length()).trim();
			if (INVOKE_TOOL_NAME.equals(toolName)) {
				return buildInvokeSkillGuidelines();
			}
		}
		if (skill.startsWith("design:")) {
			String designSkill = skill.substring("design:".length()).trim();
			String guidelines = loadClasspathSkill("design", designSkill);
			if (guidelines != null) {
				return guidelines;
			}
		}
		throw protocolError("UNKNOWN_SKILL", "Unknown skill: " + skill);
	}

	private String loadClasspathSkill(String category, String name) {
		if (category == null || name == null || category.trim().isEmpty() || name.trim().isEmpty()) {
			return null;
		}
		String resourcePath = "/skills/" + category.trim() + "/" + name.trim() + ".md";
		try {
			return EAIRepositoryUtils.loadCachedClasspathResource(MCPREST.class, resourcePath);
		}
		catch (RuntimeException e) {
			return null;
		}
	}

	private String buildInvokeSkillGuidelines() {
		List<String> sections = new ArrayList<String>();
		sections.add("Use `invoke_nabu_service` to execute any artifact with category `service`. In most cases you must provide an `input` payload that matches the definition in the service `input.xml` fragment. The output will conform to the schema in the service fragment `output.xml`.");
		sections.add("Find candidate services by searching `metadata.xml`, because service descriptions, title, summary and related documentation live there.");
		sections.add("Read the `input.xml` fragment of the target service to inspect its input schema. To understand that schema, also load the `artifact:structure` skill if you do not already have it.");
		sections.add("If `trace` is enabled on the invoke call, the returned `traceId` can be inspected with `search_nabu_service_trace`, which accepts XPath expressions to find specific parts of the execution trace. Invoke nodes use `serviceId`, and only the root invoke node carries the `traceId` attribute.");
		sections.add("Default trace structure:\n\n```xml\n<trace id=\"TRACE_ID\" serviceId=\"example.service\" started=\"2026-01-01T08:00:00Z\">\n\t<input>...</input>\n\t<invoke serviceId=\"dependency.service\" started=\"2026-01-01T08:01:00Z\">\n\t\t<input>...</input>\n\t\t<output>...</output>\n\t<error handled=\"false\">...</error>\n\t</invoke>\n\t<output>...</output>\n</trace>\n```\n\n");
		String providerGuidelines = buildTraceProviderGuidelines();
		if (providerGuidelines != null) {
			sections.add(providerGuidelines);
		}
		return String.join("\n\n", sections);
	}

	private String buildTraceProviderGuidelines() {
		List<String> guidelines = new ArrayList<String>();
		for (be.nabu.eai.repository.api.MCPTraceProvider provider : server.getRepository().getArtifacts(be.nabu.eai.repository.api.MCPTraceProvider.class)) {
			if (provider == null) {
				continue;
			}
			String single = provider.getGuidelines();
			if (single != null && !single.trim().isEmpty()) {
				guidelines.add(single.trim());
			}
		}
		if (guidelines.isEmpty()) {
			return null;
		}
		return "Additional trace provider guidance:\n\n" + String.join("\n\n", guidelines);
	}

	private String buildCreateFragmentToolDescription(List<String> dynamicArtifactTypes) {
		return "Create a new artifact fragment inside an existing artifact. This tool can ONLY be used for the following artifact types: " + String.join(", ", dynamicArtifactTypes) + ". Do NOT use this tool for any other artifact type.";
	}

	private String buildDeleteFragmentToolDescription(List<String> dynamicArtifactTypes) {
		return "Delete an existing artifact fragment inside an existing artifact. This tool can ONLY be used for the following artifact types: " + String.join(", ", dynamicArtifactTypes) + ". Do NOT use this tool for any other artifact type.";
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private DynamicArtifactFragmentManager<?> findDynamicFragmentManagerByArtifactType(String artifactType) {
		for (Class<ArtifactFragmentManager> managerClass : EAIRepositoryUtils.getImplementationsFor(server.getRepository().getClassLoader(), ArtifactFragmentManager.class, false)) {
			try {
				ArtifactFragmentManager manager = managerClass.newInstance();
				if (!(manager instanceof DynamicArtifactFragmentManager)) {
					continue;
				}
				String managerArtifactType = artifactTypeForManager(manager);
				if (artifactType.equals(managerArtifactType)) {
					return (DynamicArtifactFragmentManager<?>) manager;
				}
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return null;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private CreatableArtifactFragmentManager<?> findCreatableFragmentManagerByArtifactType(String artifactType) {
		for (Class<ArtifactFragmentManager> managerClass : EAIRepositoryUtils.getImplementationsFor(server.getRepository().getClassLoader(), ArtifactFragmentManager.class, false)) {
			try {
				ArtifactFragmentManager manager = managerClass.newInstance();
				if (!(manager instanceof CreatableArtifactFragmentManager)) {
					continue;
				}
				String managerArtifactType = artifactTypeForManager(manager);
				if (artifactType.equals(managerArtifactType)) {
					return (CreatableArtifactFragmentManager<?>) manager;
				}
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return null;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private ArtifactFragmentManager<?> findFragmentManagerByArtifactType(String artifactType) {
		for (Class<ArtifactFragmentManager> managerClass : EAIRepositoryUtils.getImplementationsFor(server.getRepository().getClassLoader(), ArtifactFragmentManager.class, false)) {
			try {
				ArtifactFragmentManager manager = managerClass.newInstance();
				String managerArtifactType = artifactTypeForManager(manager);
				if (artifactType.equals(managerArtifactType)) {
					return manager;
				}
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return null;
	}

	private Map<String, Object> createErrorResult(String code, String message) {
		Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
		structuredContent.put("code", code);
		structuredContent.put("isError", true);
		structuredContent.put("message", message);
		return structuredContent;
	}

	private Map<String, Object> createReadFragmentError(String artifactId, String path, String code, String message) {
		Map<String, Object> structuredContent = createErrorResult(code, message);
		structuredContent.put("artifactId", artifactId);
		structuredContent.put("path", path);
		return structuredContent;
	}

	private Map<String, Object> validateCreateArguments(String namespace, String name, String type, List<String> namespaces) {
		if (!isValidCreateName(name)) {
			return createErrorResult("INVALID_NAME", "Invalid artifact name '" + name + "'. Names must match the strict repository naming convention and may not use reserved names.");
		}
		if (type == null || !listCreatableArtifactTypes().contains(type)) {
			return createErrorResult("UNKNOWN_TYPE", "Unknown creatable artifact type '" + type + "'.");
		}
		String[] parts = namespace.split("\\.");
		for (String part : parts) {
			if (!isValidCreateName(part)) {
				return createErrorResult("INVALID_NAMESPACE", "Invalid namespace '" + namespace + "'. Each namespace part must match the strict repository naming convention and may not use reserved names.");
			}
		}
		String artifactId = namespace + "." + name;
		if (!isAllowedNamespace(artifactId, namespaces)) {
			return createErrorResult("INVALID_NAMESPACE", "Artifact '" + artifactId + "' is outside the allowed namespaces.");
		}
		return null;
	}

	private RepositoryEntry ensureNamespace(String namespace) throws IOException {
		EAIResourceRepository repository = EAIResourceRepository.getInstance();
		RepositoryEntry entry = repository.getRoot();
		String[] parts = namespace.split("\\.");
		for (int i = 0; i < parts.length; i++) {
			String part = parts[i];
			Entry child = entry.getChild(part);
			if (child == null) {
				if (i == 0) {
					throw new IOException("Project '" + part + "' does not exist.");
				}
				entry = entry.createDirectory(part);
			}
			else {
				if (!(child instanceof RepositoryEntry) || child.isNode()) {
					throw new IOException("An entry named '" + part + "' already exists in namespace path '" + namespace + "' and is not a folder.");
				}
				entry = (RepositoryEntry) child;
			}
		}
		return entry;
	}

	private boolean isValidCreateName(String name) {
		return name != null && EAIResourceRepository.isValidName(name) && !EAIResourceRepository.RESERVED.contains(name);
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
		List<Map<String, String>> content = textContent(toJson(structuredContent));
		return new ToolResult(structuredContent, content, buildToolMeta(null, buildFindDisplayMessage(structuredContent)));
	}

	private ToolResult readToolResult(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration) {
		Map<String, Object> structuredContent = readArtifact(arguments, meta, configuration);
		List<Map<String, String>> content = textContent(toJson(structuredContent));
		return new ToolResult(structuredContent, content, buildToolMeta(null, buildReadDisplayMessage(structuredContent)));
	}

	private ToolResult readMultipleToolResult(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration) {
		Map<String, Object> structuredContent = readMultipleArtifacts(arguments, meta, configuration);
		List<Map<String, String>> content = textContent(toJson(structuredContent));
		return new ToolResult(structuredContent, content, buildToolMeta(null, buildReadMultipleDisplayMessage(structuredContent)));
	}

	private ToolResult createFragmentToolResult(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration) {
		EditArtifactResult createResult = createArtifactFragment(arguments, meta, configuration);
		List<Map<String, String>> content = textContent(buildWriteSummaryText(createResult.structuredContent));
		Map<String, Object> toolMeta = buildToolMeta(createResult.resourceUri, buildWriteDisplayMessage(createResult.structuredContent));
		return new ToolResult(createResult.structuredContent, content, toolMeta, createResult.isError, createResult.message);
	}

	private ToolResult deleteFragmentToolResult(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration) {
		EditArtifactResult deleteResult = deleteArtifactFragment(arguments, meta, configuration);
		List<Map<String, String>> content = textContent(buildWriteSummaryText(deleteResult.structuredContent));
		Map<String, Object> toolMeta = buildToolMeta(deleteResult.resourceUri, buildWriteDisplayMessage(deleteResult.structuredContent));
		return new ToolResult(deleteResult.structuredContent, content, toolMeta, deleteResult.isError, deleteResult.message);
	}

	private ToolResult createToolResult(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration) {
		Map<String, Object> structuredContent = createArtifact(arguments, meta, configuration);
		List<Map<String, String>> content = textContent(toJson(structuredContent));
		return new ToolResult(structuredContent, content, buildToolMeta(null, buildCreateDisplayMessage(structuredContent)), asBoolean(structuredContent.get("isError")), string(structuredContent.get("message")));
	}

	private ToolResult createProjectToolResult(Map<String, Object> arguments) {
		Map<String, Object> structuredContent = createProject(arguments);
		List<Map<String, String>> content = textContent(toJson(structuredContent));
		return new ToolResult(structuredContent, content, buildToolMeta(null, buildCreateProjectDisplayMessage(structuredContent)), asBoolean(structuredContent.get("isError")), string(structuredContent.get("message")));
	}

	private ToolResult deleteToolResult(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration) {
		Map<String, Object> structuredContent = deleteArtifact(arguments, meta, configuration);
		List<Map<String, String>> content = textContent(toJson(structuredContent));
		return new ToolResult(structuredContent, content, buildToolMeta(null, buildDeleteDisplayMessage(structuredContent)), asBoolean(structuredContent.get("isError")), string(structuredContent.get("message")));
	}

	private ToolResult moveToolResult(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration) {
		Map<String, Object> structuredContent = moveArtifact(arguments, meta, configuration);
		List<Map<String, String>> content = textContent(toJson(structuredContent));
		return new ToolResult(structuredContent, content, buildToolMeta(null, buildMoveDisplayMessage(structuredContent)), asBoolean(structuredContent.get("isError")), string(structuredContent.get("message")));
	}

	private ToolResult editToolResult(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration, boolean preview) throws IOException, ParseException {
		EditArtifactResult editResult = editArtifact(arguments, meta, configuration, preview);
		List<Map<String, String>> content = textContent(buildEditSummaryText(editResult.structuredContent));
		Map<String, Object> toolMeta = buildToolMeta(editResult.resourceUri, buildEditDisplayMessage(editResult.structuredContent));
		return new ToolResult(editResult.structuredContent, content, toolMeta, editResult.isError, editResult.message);
	}

	private ToolResult writeToolResult(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration, boolean preview) throws IOException, ParseException {
		EditArtifactResult writeResult = writeArtifact(arguments, meta, configuration, preview);
		List<Map<String, String>> content = textContent(buildWriteSummaryText(writeResult.structuredContent));
		Map<String, Object> toolMeta = buildToolMeta(writeResult.resourceUri, buildWriteDisplayMessage(writeResult.structuredContent));
		return new ToolResult(writeResult.structuredContent, content, toolMeta, writeResult.isError, writeResult.message);
	}

	private ToolResult searchDocumentationToolResult(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration) {
		String pattern = requiredString(arguments, "pattern", "MISSING_PATTERN");
		int before = integer(arguments.get("beforeContext"), 0);
		int after = integer(arguments.get("afterContext"), 0);
		int offset = integer(arguments.get("offset"), 0);
		if (arguments.get("context") != null) {
			before = integer(arguments.get("context"), 0);
			after = before;
		}
		if (offset < 0) {
			throw protocolError("INVALID_OFFSET", "Argument 'offset' must be a non-negative integer.");
		}
		List<String> namespaces = resolveNamespaces(configuration, stringList(arguments.get("namespace")), meta);
		boolean caseSensitive = booleanArgument(arguments.get("caseSensitive"), false);
		int limit = integer(arguments.get("limit"), 0);
		List<DocumentationSearch> search = documentationCatalog().search(pattern, stringList(arguments.get("glob")), namespaces, caseSensitive, before, after, 0);
		List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
		for (DocumentationSearch document : search) {
			results.add(documentationMap(document, true));
		}
		int from = Math.min(offset, results.size());
		int to = limit > 0 ? Math.min(from + limit, results.size()) : results.size();
		Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
		structuredContent.put("results", new ArrayList<Map<String, Object>>(results.subList(from, to)));
		structuredContent.put("pattern", pattern);
		structuredContent.put("count", to - from);
		structuredContent.put("totalResults", results.size());
		structuredContent.put("totalMatches", countDocumentationMatches(search));
		structuredContent.put("truncated", to < results.size());
		structuredContent.put("mode", "full");
		return new ToolResult(structuredContent, textContent(toJson(structuredContent)), buildToolMeta(null, "Found " + (to - from) + " documentation result(s)."));
	}

	private ToolResult findDocumentationToolResult(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration) {
		List<String> namespaces = resolveNamespaces(configuration, null, meta);
		String namespace = string(arguments.get("namespace"));
		String pattern = string(arguments.get("pattern"));
		boolean glob = asBoolean(arguments.get("glob"));
		boolean caseSensitive = booleanArgument(arguments.get("caseSensitive"), false);
		int limit = integer(arguments.get("limit"), 200);
		int offset = integer(arguments.get("offset"), 0);
		if (limit <= 0) {
			throw protocolError("INVALID_LIMIT", "Argument 'limit' must be a positive integer.");
		}
		if (offset < 0) {
			throw protocolError("INVALID_OFFSET", "Argument 'offset' must be a non-negative integer.");
		}
		List<Map<String, Object>> files = new ArrayList<Map<String, Object>>();
		for (DocumentationSearch document : documentationCatalog().list(null, namespaces)) {
			if (namespace != null && !namespace.equals(document.getNamespace())) {
				continue;
			}
			if (pattern != null && !pattern.trim().isEmpty() && !matchesFindPattern(pattern, glob, caseSensitive, document.getNamespace(), document.getPath())) {
				continue;
			}
			files.add(documentationMap(document, false));
		}
		int total = files.size();
		int from = Math.min(offset, total);
		int to = Math.min(from + limit, total);
		Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
		structuredContent.put("files", new ArrayList<Map<String, Object>>(files.subList(from, to)));
		structuredContent.put("count", to - from);
		structuredContent.put("total", total);
		structuredContent.put("limit", limit);
		structuredContent.put("offset", offset);
		structuredContent.put("truncated", to < total);
		return new ToolResult(structuredContent, textContent(toJson(structuredContent)), buildToolMeta(null, buildFindDisplayMessage(structuredContent)));
	}

	private ToolResult readDocumentationToolResult(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration) {
		Map<String, Object> structuredContent = readDocumentation(arguments, resolveNamespaces(configuration, null, meta));
		return new ToolResult(structuredContent, textContent(toJson(structuredContent)), buildToolMeta(null, buildReadDisplayMessage(structuredContent)));
	}

	private ToolResult readMultipleDocumentationToolResult(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration) {
		List<String> namespaces = resolveNamespaces(configuration, null, meta);
		List<Object> documents = list(arguments.get("documents"));
		if (documents == null) {
			throw protocolError("MISSING_DOCUMENTS", "Missing required argument 'documents'.");
		}
		List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
		int totalBytes = 0;
		for (Object single : documents) {
			if (!(single instanceof Map)) {
				throw protocolError("INVALID_DOCUMENT", "Each entry in 'documents' must be an object.");
			}
			@SuppressWarnings("unchecked")
			Map<String, Object> document = (Map<String, Object>) single;
			Map<String, Object> result = readDocumentation(document, namespaces);
			String content = string(result.get("content"));
			totalBytes += content == null ? 0 : content.getBytes(StandardCharsets.UTF_8).length;
			if (results.size() >= MAX_MULTI_READ_FRAGMENTS || totalBytes > MAX_MULTI_READ_BYTES) {
				break;
			}
			results.add(result);
		}
		Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
		structuredContent.put("documents", results);
		structuredContent.put("count", results.size());
		structuredContent.put("totalRequested", documents.size());
		structuredContent.put("truncated", results.size() < documents.size());
		return new ToolResult(structuredContent, textContent(toJson(structuredContent)), buildToolMeta(null, buildReadMultipleDisplayMessage(structuredContent)));
	}

	private ToolResult editDocumentationToolResult(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration, boolean preview) throws IOException, ParseException {
		EditArtifactResult editResult = editDocumentation(arguments, meta, configuration, preview);
		return new ToolResult(editResult.structuredContent, textContent(buildEditSummaryText(editResult.structuredContent)), buildToolMeta(editResult.resourceUri, buildEditDisplayMessage(editResult.structuredContent)), editResult.isError, editResult.message);
	}

	private ToolResult writeDocumentationToolResult(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration, boolean preview) throws IOException, ParseException {
		EditArtifactResult writeResult = writeDocumentation(arguments, meta, configuration, preview);
		return new ToolResult(writeResult.structuredContent, textContent(buildWriteSummaryText(writeResult.structuredContent)), buildToolMeta(writeResult.resourceUri, buildWriteDisplayMessage(writeResult.structuredContent)), writeResult.isError, writeResult.message);
	}

	private ToolResult deleteDocumentationToolResult(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration, boolean preview) throws IOException {
		EditArtifactResult deleteResult = deleteDocumentation(arguments, meta, configuration, preview);
		return new ToolResult(deleteResult.structuredContent, textContent(buildWriteSummaryText(deleteResult.structuredContent)), buildToolMeta(deleteResult.resourceUri, buildWriteDisplayMessage(deleteResult.structuredContent)), deleteResult.isError, deleteResult.message);
	}

	private EditArtifactResult editDocumentation(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration, boolean preview) throws IOException, ParseException {
		List<String> namespaces = resolveNamespaces(configuration, null, meta);
		String namespace = requiredString(arguments, "namespace", "MISSING_NAMESPACE");
		String path = requiredString(arguments, "path", "MISSING_PATH");
		assertAllowedDocumentation(namespace, path, namespaces);
		DocumentationSearch document = requiredDocumentation(namespace, path);
		if (!document.isEditable()) {
			throw protocolError("NOT_EDITABLE", "Documentation is not editable: '" + namespace + "' at path '" + path + "'.");
		}
		String before = document.getContent() == null ? "" : document.getContent();
		String after = before;
		int totalMatchCount = 0;
		for (Map<String, String> edit : extractEdits(arguments, namespace, path)) {
			String find = edit.get("find");
			int matchCount = countMatches(after, find);
			if (matchCount == 0) {
				throw protocolError("FIND_NOT_FOUND", "No match found for the requested 'find' text in documentation '" + namespace + "' at path '" + path + "'.");
			}
			if (matchCount > 1) {
				throw protocolError("FIND_NOT_UNIQUE", "The requested 'find' text matches multiple locations in documentation '" + namespace + "' at path '" + path + "'. Provide a more specific match.");
			}
			after = after.replace(find, edit.get("replace"));
			totalMatchCount += matchCount;
		}
		return finishDocumentationWrite(namespace, path, before, after, totalMatchCount, preview);
	}

	private EditArtifactResult writeDocumentation(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration, boolean preview) throws IOException, ParseException {
		List<String> namespaces = resolveNamespaces(configuration, null, meta);
		String namespace = requiredString(arguments, "namespace", "MISSING_NAMESPACE");
		String path = requiredString(arguments, "path", "MISSING_PATH");
		String content = string(arguments.get("content"));
		String mode = string(arguments.get("mode"));
		if (mode == null) {
			mode = "overwrite";
		}
		if (!"overwrite".equals(mode) && !"append".equals(mode) && !"prepend".equals(mode)) {
			throw protocolError("INVALID_MODE", "Mode must be one of: overwrite, append, prepend.");
		}
		assertAllowedDocumentation(namespace, path, namespaces);
		DocumentationSearch existing = documentationCatalog().get(namespace, path);
		if (existing != null && !existing.isEditable()) {
			throw protocolError("NOT_EDITABLE", "Documentation is not editable: '" + namespace + "' at path '" + path + "'.");
		}
		String before = existing == null || existing.getContent() == null ? "" : existing.getContent();
		String after = "append".equals(mode) ? before + content : ("prepend".equals(mode) ? content + before : content);
		return finishDocumentationWrite(namespace, path, before, after, 1, preview);
	}

	private EditArtifactResult deleteDocumentation(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration, boolean preview) throws IOException {
		List<String> namespaces = resolveNamespaces(configuration, null, meta);
		String namespace = requiredString(arguments, "namespace", "MISSING_NAMESPACE");
		String path = requiredString(arguments, "path", "MISSING_PATH");
		assertAllowedDocumentation(namespace, path, namespaces);
		DocumentationSearch document = requiredDocumentation(namespace, path);
		if (!document.isRemovable()) {
			throw protocolError("NOT_REMOVABLE", "Documentation is not removable: '" + namespace + "' at path '" + path + "'.");
		}
		Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
		structuredContent.put("namespace", namespace);
		structuredContent.put("path", path);
		structuredContent.put("count", 1);
		structuredContent.put("updatedCount", preview ? 1 : 0);
		structuredContent.put("failedCount", 0);
		structuredContent.put("message", preview ? "Preview delete documentation '" + path + "'." : "Deleted documentation '" + path + "'.");
		if (!preview) {
			documentationCatalog().delete(namespace, path);
		}
		ensureStaticReviewResource();
		return new EditArtifactResult(structuredContent, REVIEW_RESOURCE_URI, false, null);
	}

	private EditArtifactResult finishDocumentationWrite(String namespace, String path, String before, String after, int matchCount, boolean preview) throws IOException {
		Map<String, Object> operation = new LinkedHashMap<String, Object>();
		operation.put("artifactId", namespace);
		operation.put("path", path);
		operation.put("before", before);
		operation.put("after", after);
		operation.put("matchCount", matchCount);
		Map<String, Object> update = new LinkedHashMap<String, Object>();
		update.put("namespace", namespace);
		update.put("path", path);
		update.put("matchCount", matchCount);
		update.put("original", before);
		update.put("new", after);
		update.put("diff", buildFallbackDiff(Arrays.asList(operation)));
		int successCount = preview ? 1 : 0;
		if (!preview) {
			documentationCatalog().write(namespace, path, after, "overwrite");
			successCount = 1;
		}
		Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
		structuredContent.put("namespace", namespace);
		structuredContent.put("path", path);
		structuredContent.put("matchCount", matchCount);
		structuredContent.put("original", before);
		structuredContent.put("new", after);
		structuredContent.put("diff", update.get("diff"));
		structuredContent.put("count", 1);
		structuredContent.put("updatedCount", successCount);
		structuredContent.put("failedCount", 1 - successCount);
		structuredContent.put("updates", Arrays.asList(update));
		structuredContent.put("isError", false);
		ensureStaticReviewResource();
		return new EditArtifactResult(structuredContent, REVIEW_RESOURCE_URI, false, null);
	}

	private Map<String, Object> readDocumentation(Map<String, Object> arguments, List<String> namespaces) {
		String namespace = requiredString(arguments, "namespace", "MISSING_NAMESPACE");
		String path = requiredString(arguments, "path", "MISSING_PATH");
		assertAllowedDocumentation(namespace, path, namespaces);
		int startLine = integer(arguments.get("startLine"), 1);
		int limit = integer(arguments.get("limit"), 200);
		if (startLine <= 0) {
			throw protocolError("INVALID_START_LINE", "Argument 'startLine' must be a positive integer.");
		}
		if (limit <= 0) {
			throw protocolError("INVALID_LIMIT", "Argument 'limit' must be a positive integer.");
		}
		DocumentationSearch document = requiredDocumentation(namespace, path);
		Map<String, Object> structuredContent = readLines(document.getContent() == null ? "" : document.getContent(), path, startLine, limit);
		structuredContent.put("namespace", namespace);
		structuredContent.put("editable", document.isEditable());
		structuredContent.put("removable", document.isRemovable());
		return structuredContent;
	}

	private Map<String, Object> readLines(String content, String path, int startLine, int limit) {
		String[] lines = content.split("\\r?\\n", -1);
		int total = lines.length;
		Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
		structuredContent.put("path", path);
		structuredContent.put("startLine", startLine);
		structuredContent.put("total", total);
		if (startLine > total) {
			structuredContent.put("count", 0);
			structuredContent.put("content", "");
			structuredContent.put("code", "EMPTY_RANGE");
			return structuredContent;
		}
		int from = startLine - 1;
		StringBuilder builder = new StringBuilder();
		List<String> truncatedReason = new ArrayList<String>();
		int count = 0;
		int totalBytes = 0;
		boolean truncated = false;
		for (int i = from; i < lines.length; i++) {
			if (count >= limit) {
				truncated = true;
				if (!truncatedReason.contains("line_limit")) {
					truncatedReason.add("line_limit");
				}
				break;
			}
			String line = lines[i];
			int lineBytes = line.getBytes(StandardCharsets.UTF_8).length;
			if (lineBytes > MAX_READ_LINE_BYTES) {
				String truncatedLine = truncateToBytes(line, MAX_READ_LINE_BYTES);
				int keptBytes = truncatedLine.getBytes(StandardCharsets.UTF_8).length;
				line = truncatedLine + " [TRUNCATED: " + (lineBytes - keptBytes) + " bytes hidden]";
				truncated = true;
				if (!truncatedReason.contains("long_lines")) {
					truncatedReason.add("long_lines");
				}
			}
			String formatted = builder.length() == 0 ? line : "\n" + line;
			int formattedBytes = formatted.getBytes(StandardCharsets.UTF_8).length;
			if (totalBytes + formattedBytes > MAX_READ_BYTES) {
				truncated = true;
				if (!truncatedReason.contains("max_bytes")) {
					truncatedReason.add("max_bytes");
				}
				break;
			}
			builder.append(formatted);
			totalBytes += formattedBytes;
			count++;
		}
		if (from + count < lines.length) {
			truncated = true;
			if (!truncatedReason.contains("line_limit") && count >= limit) {
				truncatedReason.add("line_limit");
			}
		}
		structuredContent.put("count", count);
		structuredContent.put("content", builder.toString());
		structuredContent.put("truncated", truncated);
		if (truncated) {
			structuredContent.put("truncated_reason", truncatedReason);
		}
		return structuredContent;
	}

	private Map<String, Object> documentationMap(DocumentationSearch document, boolean includeMatches) {
		Map<String, Object> entry = new LinkedHashMap<String, Object>();
		entry.put("namespace", document.getNamespace());
		entry.put("path", document.getPath());
		entry.put("contentType", document.getContentType());
		entry.put("editable", document.isEditable());
		entry.put("removable", document.isRemovable());
		if (document.getProperties() != null && !document.getProperties().isEmpty()) {
			entry.put("properties", document.getProperties());
		}
		if (includeMatches) {
			entry.put("matches", groupMatches(document.getMatches()));
		}
		return entry;
	}

	private int countDocumentationMatches(List<DocumentationSearch> documents) {
		int count = 0;
		for (DocumentationSearch document : documents) {
			count += groupMatches(document.getMatches()).size();
		}
		return count;
	}

	private DocumentationSearch requiredDocumentation(String namespace, String path) {
		DocumentationSearch document = documentationCatalog().get(namespace, path);
		if (document == null) {
			throw protocolError("INVALID_PATH", "Documentation not found for namespace '" + namespace + "' at path '" + path + "'.");
		}
		return document;
	}

	private void assertAllowedDocumentation(String namespace, String path, List<String> namespaces) {
		if (!isAllowedNamespace(namespace, namespaces)) {
			throw protocolError("INVALID_PATH", "Namespace '" + namespace + "' is outside the allowed namespaces.");
		}
		if (path == null || path.startsWith("/") || path.contains("../") || path.equals("..") || path.contains("/..")) {
			throw protocolError("INVALID_PATH", "Invalid documentation path: '" + path + "'.");
		}
	}

	private DocumentationCatalogService documentationCatalog() {
		DocumentationCatalogService service = server.getDocumentationCatalogService();
		if (service == null) {
			throw new HTTPException(503, "The documentation catalog is unavailable.");
		}
		return service;
	}

	private EditArtifactResult editArtifact(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration, boolean preview) throws IOException, ParseException {
		List<String> namespaces = resolveNamespaces(configuration, null, meta);
		String artifactId = requiredString(arguments, "artifactId", "MISSING_ARTIFACT_ID");
		String path = requiredString(arguments, "path", "MISSING_PATH");
		if (!isAllowedNamespace(artifactId, namespaces)) {
			throw protocolError("INVALID_PATH", "Artifact '" + artifactId + "' is outside the allowed namespaces.");
		}
		Node currentNode = server.getRepository().getNode(artifactId);
		if (currentNode == null) {
			throw protocolError("INVALID_PATH", "Artifact not found: '" + artifactId + "'.");
		}
		Artifact currentArtifact = (Artifact) currentNode.getArtifact();
		ArtifactFragmentManager<Artifact> currentManager = EAIRepositoryUtils.getArtifactFragmentManager(currentArtifact);
		if (currentManager == null) {
			throw protocolError("INVALID_PATH", "Artifact '" + artifactId + "' does not support fragment access.");
		}
		ArtifactFragment currentFragment = findEditableFragment(currentManager, currentArtifact, artifactId, path);
		String before = currentFragment.getContent();
		String after = before;
		int totalMatchCount = 0;
		for (Map<String, String> edit : extractEdits(arguments, artifactId, path)) {
			String find = edit.get("find");
			int matchCount = countMatches(after, find);
			if (matchCount == 0) {
				throw protocolError("FIND_NOT_FOUND", "No match found for the requested 'find' text in artifact '" + artifactId + "' at path '" + path + "'.");
			}
			if (matchCount > 1) {
				throw protocolError("FIND_NOT_UNIQUE", "The requested 'find' text matches multiple locations in artifact '" + artifactId + "' at path '" + path + "'. Provide a more specific match.");
			}
			after = after.replace(find, edit.get("replace"));
			totalMatchCount += matchCount;
		}
		Map<String, Object> operation = new LinkedHashMap<String, Object>();
		operation.put("artifactId", artifactId);
		operation.put("path", path);
		operation.put("artifact", currentArtifact);
		operation.put("manager", currentManager);
		operation.put("before", before);
		operation.put("after", after);
		operation.put("matchCount", totalMatchCount);
		Map<String, Object> update = new LinkedHashMap<String, Object>();
		update.put("artifactId", artifactId);
		update.put("path", path);
		update.put("matchCount", totalMatchCount);
		update.put("original", before);
		update.put("new", after);
		update.put("diff", buildFallbackDiff(Arrays.asList(operation)));
		int successCount = 0;
		if (preview) {
			successCount = 1;
		}
		else {
			try {
				List<Validation<?>> validations = currentManager.updateFragment(currentArtifact, path, before, after);
				if (validations != null && !validations.isEmpty()) {
					update.put("validations", validationMaps(validations));
				}
				if (hasErrors(validations)) {
					update.put("error", buildValidationMessage(validations));
				}
				else {
					reloadArtifactAfterMcpUpdate(currentManager, artifactId, path);
					successCount = 1;
				}
			}
			catch (Exception e) {
				update.put("error", e.getMessage() == null ? e.getClass().getName() : e.getMessage());
			}
		}
		Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
		structuredContent.put("path", path);
		structuredContent.put("matchCount", totalMatchCount);
		structuredContent.put("original", before);
		structuredContent.put("new", after);
		structuredContent.put("diff", update.get("diff"));
		structuredContent.put("count", 1);
		structuredContent.put("updatedCount", successCount);
		structuredContent.put("failedCount", 1 - successCount);
		structuredContent.put("updates", Arrays.asList(update));
		String message = update.get("error") == null ? null : update.get("error").toString();
		boolean isError = successCount == 0;
		structuredContent.put("isError", isError);
		if (message != null) {
			structuredContent.put("message", message);
		}
		if (update.get("validations") != null) {
			structuredContent.put("validations", update.get("validations"));
		}
		ensureStaticReviewResource();
		return new EditArtifactResult(structuredContent, REVIEW_RESOURCE_URI, Boolean.valueOf(isError), message);
	}

	private Map<String, Object> findArtifactFragments(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration) {
		List<String> namespaces = resolveNamespaces(configuration, stringList(arguments.get("namespace")), meta);
		String artifactId = string(arguments.get("artifactId"));
		List<String> artifactTypes = stringList(arguments.get("artifactType"));
		List<String> artifactCategories = stringList(arguments.get("artifactCategory"));
		String pattern = string(arguments.get("pattern"));
		boolean glob = asBoolean(arguments.get("glob"));
		boolean caseSensitive = booleanArgument(arguments.get("caseSensitive"), false);
		int limit = integer(arguments.get("limit"), 200);
		int offset = integer(arguments.get("offset"), 0);
		if (limit <= 0) {
			throw protocolError("INVALID_LIMIT", "Argument 'limit' must be a positive integer.");
		}
		if (offset < 0) {
			throw protocolError("INVALID_OFFSET", "Argument 'offset' must be a non-negative integer.");
		}
		List<FragmentSearch> fragments = server.getFragmentIndexService() == null
			? Collections.<FragmentSearch>emptyList()
			: server.getFragmentIndexService().list(null, namespaces, artifactTypes, artifactCategories);
		List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
		for (FragmentSearch fragment : fragments) {
			if (artifactId != null && !artifactId.equals(fragment.getArtifactId())) {
				continue;
			}
			if (pattern != null && !pattern.trim().isEmpty() && !matchesFindPattern(pattern, glob, caseSensitive, fragment.getArtifactId(), fragment.getPath())) {
				continue;
			}
			Map<String, Object> entry = new LinkedHashMap<String, Object>();
			entry.put("artifactId", fragment.getArtifactId());
			entry.put("path", fragment.getPath());
			entry.put("artifactType", fragment.getArtifactType());
			entry.put("artifactCategory", fragment.getArtifactCategory());
			entry.put("fragmentType", fragment.getFragmentType());
			entry.put("contentType", fragment.getContentType());
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

	private Map<String, Object> createArtifact(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration) {
		List<String> namespaces = resolveNamespaces(configuration, null, meta);
		String namespace = requiredString(arguments, "namespace", "MISSING_NAMESPACE");
		String name = requiredString(arguments, "name", "MISSING_NAME");
		String type = requiredString(arguments, "type", "MISSING_TYPE");
		Map<String, Object> invalid = validateCreateArguments(namespace, name, type, namespaces);
		if (invalid != null) {
			return invalid;
		}
		CreatableArtifactFragmentManager<?> manager = findCreatableFragmentManagerByArtifactType(type);
		if (manager == null) {
			return createErrorResult("UNKNOWN_TYPE", "No creatable artifact manager found for type '" + type + "'.");
		}
		try {
			RepositoryEntry parent = ensureNamespace(namespace);
			if (parent.getChild(name) != null) {
				return createErrorResult("NAME_EXISTS", "An entry named '" + name + "' already exists in namespace '" + namespace + "'.");
			}
			manager.createArtifact(parent, name);
			String artifactId = parent.getId() + "." + name;
			reloadArtifactAfterMcpCreate(parent.getId());
			notifyCollaborationCreate(artifactId);
			Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
			structuredContent.put("code", "CREATED");
			structuredContent.put("artifactId", artifactId);
			structuredContent.put("namespace", namespace);
			structuredContent.put("name", name);
			structuredContent.put("type", type);
			return structuredContent;
		}
		catch (Exception e) {
			return createErrorResult("CREATE_FAILED", firstExceptionMessage(e));
		}
	}

	private Map<String, Object> deleteArtifact(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration) {
		List<String> namespaces = resolveNamespaces(configuration, null, meta);
		String artifactId = requiredString(arguments, "artifactId", "MISSING_ARTIFACT_ID");
		if (!isAllowedNamespace(artifactId, namespaces)) {
			return createErrorResult("INVALID_PATH", "Artifact deletion must stay within the allowed namespaces.");
		}
		EAIResourceRepository repository = EAIResourceRepository.getInstance();
		Entry entry = repository.getEntry(artifactId);
		if (entry == null) {
			return createErrorResult("INVALID_PATH", "Entry not found: '" + artifactId + "'.");
		}
		if (entry.getParent() == null) {
			return createErrorResult("INVALID_PATH", "Can not delete repository root entry '" + artifactId + "'.");
		}
		boolean deletable = entry.getParent() instanceof ExtensibleEntry;
		if (!deletable && entry instanceof ResourceEntry && ((ResourceEntry) entry).getContainer() != null && ((ResourceEntry) entry).getContainer().getParent() instanceof ManageableContainer) {
			deletable = true;
		}
		if (!deletable) {
			return createErrorResult("NOT_DELETABLE", "Entry '" + artifactId + "' is not deletable.");
		}
		try {
			List<String> dependenciesToReload = repository.getDependencies(artifactId);
			if (entry.getParent() instanceof ExtensibleEntry) {
				((ExtensibleEntry) entry.getParent()).deleteChild(entry.getName(), true);
			}
			else {
				repository.unload(artifactId);
				((ManageableContainer<?>) ((ResourceEntry) entry).getContainer().getParent()).delete(entry.getName());
				repository.reload(entry.getParent().getId());
			}
			reloadArtifactsAfterMcpDelete(dependenciesToReload);
			if (server.getCollaborationListener() != null) {
				server.getCollaborationListener().notifyArtifactReload(artifactId, "MCP deleted");
			}
			Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
			structuredContent.put("code", "DELETED");
			structuredContent.put("artifactId", artifactId);
			structuredContent.put("success", true);
			return structuredContent;
		}
		catch (Exception e) {
			LOGGER.error("Could not delete artifact " + artifactId, e);
			return createErrorResult("DELETE_FAILED", firstExceptionMessage(e));
		}
	}

	private Map<String, Object> moveArtifact(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration) {
		List<String> namespaces = resolveNamespaces(configuration, null, meta);
		String oldId = requiredString(arguments, "oldId", "MISSING_OLD_ID");
		String newId = requiredString(arguments, "newId", "MISSING_NEW_ID");
		if (!isAllowedNamespace(oldId, namespaces) || !isAllowedNamespace(newId, namespaces)) {
			return createErrorResult("INVALID_PATH", "Artifact move must stay within the allowed namespaces.");
		}
		EAIResourceRepository repository = EAIResourceRepository.getInstance();
		Entry sourceEntry = repository.getEntry(oldId);
		if (sourceEntry == null) {
			return createErrorResult("INVALID_PATH", "Entry not found: '" + oldId + "'.");
		}
		if (repository.getEntry(newId) != null) {
			return createErrorResult("INVALID_PATH", "Entry already exists: '" + newId + "'.");
		}
		String targetName = newId.contains(".") ? newId.replaceAll(".*\\.([^.]+)$", "$1") : newId;
		if (!isValidCreateName(targetName)) {
			return createErrorResult("INVALID_NAME", "Invalid target artifact name '" + targetName + "'. Names must match the strict repository naming convention and may not use reserved names.");
		}
		try {
			List<Validation<?>> validations = repository.move(oldId, newId, true);
			Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
			structuredContent.put("oldId", oldId);
			structuredContent.put("newId", newId);
			structuredContent.put("validationCount", validations == null ? 0 : validations.size());
			structuredContent.put("validations", validationMaps(validations == null ? Collections.<Validation<?>>emptyList() : validations));
			structuredContent.put("success", true);
			return structuredContent;
		}
		catch (Exception e) {
			return createErrorResult("MOVE_FAILED", firstExceptionMessage(e));
		}
	}

	private Map<String, Object> createProject(Map<String, Object> arguments) {
		String name = requiredString(arguments, "name", "MISSING_NAME");
		String type = requiredString(arguments, "type", "MISSING_TYPE");
		if (!isValidCreateName(name)) {
			return createErrorResult("INVALID_NAME", "Invalid project name '" + name + "'. Names must match the strict repository naming convention and may not use reserved names.");
		}
		ProjectType projectType;
		try {
			projectType = ProjectType.valueOf(type.trim().toUpperCase());
		}
		catch (Exception e) {
			return createErrorResult("UNKNOWN_TYPE", "Unknown project type '" + type + "'.");
		}
		try {
			RepositoryEntry root = EAIResourceRepository.getInstance().getRoot();
			if (root.getChild(name) != null) {
				return createErrorResult("NAME_EXISTS", "A project or artifact named '" + name + "' already exists.");
			}
			RepositoryEntry newEntry = root.createDirectory(name);
			CollectionImpl collection = new CollectionImpl();
			collection.setType("project");
			collection.setSubType(projectType.name().toLowerCase());
			newEntry.setCollection(collection);
			newEntry.saveCollection();
			root.refresh(true);
			reloadArtifactAfterMcpCreate(newEntry.getId());
			notifyCollaborationCreate(newEntry.getId());
			Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
			structuredContent.put("code", "CREATED");
			structuredContent.put("projectId", newEntry.getId());
			structuredContent.put("name", name);
			structuredContent.put("type", projectType.name().toLowerCase());
			return structuredContent;
		}
		catch (Exception e) {
			return createErrorResult("CREATE_FAILED", firstExceptionMessage(e));
		}
	}

	private Map<String, Object> readArtifact(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration) {
		List<String> namespaces = resolveNamespaces(configuration, null, meta);
		return readArtifact(arguments, namespaces);
	}

	private Map<String, Object> readMultipleArtifacts(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration) {
		List<String> namespaces = resolveNamespaces(configuration, null, meta);
		List<Object> fragments = list(arguments.get("fragments"));
		if (fragments == null || fragments.isEmpty()) {
			throw protocolError("MISSING_FRAGMENTS", "Argument 'fragments' must contain at least one fragment request.");
		}
		if (fragments.size() > MAX_MULTI_READ_FRAGMENTS) {
			throw protocolError("TOO_MANY_FRAGMENTS", "Argument 'fragments' may contain at most " + MAX_MULTI_READ_FRAGMENTS + " fragment requests.");
		}
		List<Map<String, Object>> requests = new ArrayList<Map<String, Object>>();
		List<String> artifactIds = new ArrayList<String>();
		List<String> paths = new ArrayList<String>();
		for (Object object : fragments) {
			Map<String, Object> fragment = map(object);
			if (fragment == null) {
				throw protocolError("INVALID_FRAGMENT", "Each entry in 'fragments' must be an object.");
			}
			String artifactId = requiredString(fragment, "artifactId", "MISSING_ARTIFACT_ID");
			String path = requiredString(fragment, "path", "MISSING_PATH");
			requests.add(fragment);
			if (!artifactIds.contains(artifactId)) {
				artifactIds.add(artifactId);
			}
			if (!paths.contains(path)) {
				paths.add(path);
			}
		}
		Map<String, FragmentSearch> indexedFragments = indexFragments(getIndexedFragments(artifactIds, paths, namespaces));
		List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> errors = new ArrayList<Map<String, Object>>();
		int totalBytes = 0;
		boolean truncated = false;
		for (Map<String, Object> request : requests) {
			String artifactId = requiredString(request, "artifactId", "MISSING_ARTIFACT_ID");
			String path = requiredString(request, "path", "MISSING_PATH");
			FragmentSearch fragment = indexedFragments.get(fragmentKey(artifactId, path));
			if (fragment == null) {
				errors.add(createReadFragmentError(artifactId, path, "INVALID_PATH", "Fragment not found for artifact '" + artifactId + "' at path '" + path + "'."));
				continue;
			}
			Map<String, Object> result = readArtifact(request, fragment);
			String fragmentContent = string(result.get("content"));
			int fragmentBytes = fragmentContent == null ? 0 : fragmentContent.getBytes(StandardCharsets.UTF_8).length;
			if (!results.isEmpty() && totalBytes + fragmentBytes > MAX_MULTI_READ_BYTES) {
				truncated = true;
				break;
			}
			results.add(result);
			totalBytes += fragmentBytes;
		}
		if (results.isEmpty() && !errors.isEmpty()) {
			Map<String, Object> firstError = errors.get(0);
			throw protocolError(string(firstError.get("code")), string(firstError.get("message")));
		}
		Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
		structuredContent.put("fragments", results);
		structuredContent.put("errors", errors);
		structuredContent.put("count", results.size());
		structuredContent.put("errorCount", errors.size());
		structuredContent.put("totalRequested", fragments.size());
		structuredContent.put("truncated", truncated);
		structuredContent.put("totalBytes", totalBytes);
		return structuredContent;
	}

	private Map<String, Object> readArtifact(Map<String, Object> arguments, List<String> namespaces) {
		String artifactId = requiredString(arguments, "artifactId", "MISSING_ARTIFACT_ID");
		String path = requiredString(arguments, "path", "MISSING_PATH");
		return readArtifact(arguments, getIndexedFragment(artifactId, path, namespaces));
	}

	private Map<String, Object> readArtifact(Map<String, Object> arguments, Map<String, FragmentSearch> indexedFragments) {
		String artifactId = requiredString(arguments, "artifactId", "MISSING_ARTIFACT_ID");
		String path = requiredString(arguments, "path", "MISSING_PATH");
		FragmentSearch fragment = indexedFragments.get(fragmentKey(artifactId, path));
		if (fragment == null) {
			throw protocolError("INVALID_PATH", "Fragment not found for artifact '" + artifactId + "' at path '" + path + "'.");
		}
		return readArtifact(arguments, fragment);
	}

	private Map<String, Object> readArtifact(Map<String, Object> arguments, FragmentSearch fragment) {
		String artifactId = requiredString(arguments, "artifactId", "MISSING_ARTIFACT_ID");
		String path = requiredString(arguments, "path", "MISSING_PATH");
		int startLine = integer(arguments.get("startLine"), 1);
		int limit = integer(arguments.get("limit"), 200);
		if (startLine <= 0) {
			throw protocolError("INVALID_START_LINE", "Argument 'startLine' must be a positive integer.");
		}
		if (limit <= 0) {
			throw protocolError("INVALID_LIMIT", "Argument 'limit' must be a positive integer.");
		}
		String content = fragment.getContent() == null ? "" : fragment.getContent();
		Map<String, Object> structuredContent = readLines(content, path, startLine, limit);
		structuredContent.put("artifactId", artifactId);
		return structuredContent;
	}

	@SuppressWarnings("unchecked")
	private EditArtifactResult createArtifactFragment(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration) {
		List<String> namespaces = resolveNamespaces(configuration, null, meta);
		String artifactId = requiredString(arguments, "artifactId", "MISSING_ARTIFACT_ID");
		String path = requiredString(arguments, "path", "MISSING_PATH");
		String content = string(arguments.get("content"));
		Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
		structuredContent.put("artifactId", artifactId);
		structuredContent.put("path", path);
		if (!isAllowedNamespace(artifactId, namespaces)) {
			String message = "Artifact '" + artifactId + "' is outside the allowed namespaces.";
			structuredContent.put("isError", true);
			structuredContent.put("message", message);
			ensureStaticReviewResource();
			return new EditArtifactResult(structuredContent, REVIEW_RESOURCE_URI, true, message);
		}
		Node currentNode = server.getRepository().getNode(artifactId);
		if (currentNode == null) {
			String message = "Artifact not found: '" + artifactId + "'.";
			structuredContent.put("isError", true);
			structuredContent.put("message", message);
			ensureStaticReviewResource();
			return new EditArtifactResult(structuredContent, REVIEW_RESOURCE_URI, true, message);
		}
		try {
			Artifact currentArtifact = (Artifact) currentNode.getArtifact();
			ArtifactFragmentManager<Artifact> currentManager = EAIRepositoryUtils.getArtifactFragmentManager(currentArtifact);
			if (currentManager == null) {
				throw protocolError("INVALID_PATH", "Artifact '" + artifactId + "' does not support fragment access.");
			}
			String artifactType = artifactTypeForManager(currentManager);
			DynamicArtifactFragmentManager<Artifact> dynamicManager = (DynamicArtifactFragmentManager<Artifact>) findDynamicFragmentManagerByArtifactType(artifactType);
			if (dynamicManager == null) {
				throw protocolError("INVALID_PATH", "Artifact '" + artifactId + "' does not support dynamic fragment creation.");
			}
			List<Validation<?>> validations = dynamicManager.createFragment(currentArtifact, path, content);
			boolean isError = hasErrors(validations);
			if (!isError) {
				reloadArtifactAfterMcpUpdate(dynamicManager, artifactId, path);
			}
			String message = buildValidationMessage(validations);
			structuredContent.put("code", isError ? "CREATE_FAILED" : "CREATED");
			structuredContent.put("isError", isError);
			structuredContent.put("message", validations == null || validations.isEmpty() ? (isError ? "Failed to create fragment." : "Created fragment '" + path + "'.") : message);
			structuredContent.put("validations", validationMaps(validations == null ? Collections.<Validation<?>>emptyList() : validations));
			ensureStaticReviewResource();
			return new EditArtifactResult(structuredContent, REVIEW_RESOURCE_URI, Boolean.valueOf(isError), string(structuredContent.get("message")));
		}
		catch (Exception e) {
			String message = e.getMessage() == null ? e.getClass().getName() : e.getMessage();
			structuredContent.put("isError", true);
			structuredContent.put("message", message);
			ensureStaticReviewResource();
			return new EditArtifactResult(structuredContent, REVIEW_RESOURCE_URI, true, message);
		}
	}

	@SuppressWarnings("unchecked")
	private EditArtifactResult deleteArtifactFragment(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration) {
		List<String> namespaces = resolveNamespaces(configuration, null, meta);
		String artifactId = requiredString(arguments, "artifactId", "MISSING_ARTIFACT_ID");
		String path = requiredString(arguments, "path", "MISSING_PATH");
		Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
		structuredContent.put("artifactId", artifactId);
		structuredContent.put("path", path);
		if (!isAllowedNamespace(artifactId, namespaces)) {
			String message = "Artifact '" + artifactId + "' is outside the allowed namespaces.";
			structuredContent.put("isError", true);
			structuredContent.put("message", message);
			ensureStaticReviewResource();
			return new EditArtifactResult(structuredContent, REVIEW_RESOURCE_URI, true, message);
		}
		Node currentNode = server.getRepository().getNode(artifactId);
		if (currentNode == null) {
			String message = "Artifact not found: '" + artifactId + "'.";
			structuredContent.put("isError", true);
			structuredContent.put("message", message);
			ensureStaticReviewResource();
			return new EditArtifactResult(structuredContent, REVIEW_RESOURCE_URI, true, message);
		}
		try {
			Artifact currentArtifact = (Artifact) currentNode.getArtifact();
			ArtifactFragmentManager<Artifact> currentManager = EAIRepositoryUtils.getArtifactFragmentManager(currentArtifact);
			if (currentManager == null) {
				throw protocolError("INVALID_PATH", "Artifact '" + artifactId + "' does not support fragment access.");
			}
			String artifactType = artifactTypeForManager(currentManager);
			DynamicArtifactFragmentManager<Artifact> dynamicManager = (DynamicArtifactFragmentManager<Artifact>) findDynamicFragmentManagerByArtifactType(artifactType);
			if (dynamicManager == null) {
				throw protocolError("INVALID_PATH", "Artifact '" + artifactId + "' does not support dynamic fragment deletion.");
			}
			List<Validation<?>> validations = dynamicManager.deleteFragment(currentArtifact, path);
			boolean isError = hasErrors(validations);
			if (!isError) {
				reloadArtifactAfterMcpUpdate(dynamicManager, artifactId, path);
			}
			String message = buildValidationMessage(validations);
			structuredContent.put("code", isError ? "DELETE_FAILED" : "DELETED");
			structuredContent.put("isError", isError);
			structuredContent.put("message", validations == null || validations.isEmpty() ? (isError ? "Failed to delete fragment." : "Deleted fragment '" + path + "'.") : message);
			structuredContent.put("validations", validationMaps(validations == null ? Collections.<Validation<?>>emptyList() : validations));
			ensureStaticReviewResource();
			return new EditArtifactResult(structuredContent, REVIEW_RESOURCE_URI, Boolean.valueOf(isError), string(structuredContent.get("message")));
		}
		catch (Exception e) {
			String message = e.getMessage() == null ? e.getClass().getName() : e.getMessage();
			structuredContent.put("isError", true);
			structuredContent.put("message", message);
			ensureStaticReviewResource();
			return new EditArtifactResult(structuredContent, REVIEW_RESOURCE_URI, true, message);
		}
	}

	private EditArtifactResult writeArtifact(Map<String, Object> arguments, Map<String, Object> meta, MCPConfiguration configuration, boolean preview) {
		List<String> namespaces = resolveNamespaces(configuration, null, meta);
		String artifactId = requiredString(arguments, "artifactId", "MISSING_ARTIFACT_ID");
		String path = requiredString(arguments, "path", "MISSING_PATH");
		String content = requiredString(arguments, "content", "MISSING_CONTENT");
		String mode = string(arguments.get("mode"));
		if (mode == null) {
			mode = "overwrite";
		}
		if (!"overwrite".equals(mode) && !"append".equals(mode) && !"prepend".equals(mode)) {
			throw protocolError("INVALID_MODE", "Argument 'mode' must be one of: overwrite, append, prepend.");
		}
		if (!isAllowedNamespace(artifactId, namespaces)) {
			throw protocolError("INVALID_PATH", "Artifact '" + artifactId + "' is outside the allowed namespaces.");
		}
		Node currentNode = server.getRepository().getNode(artifactId);
		if (currentNode == null) {
			throw protocolError("INVALID_PATH", "Artifact not found: '" + artifactId + "'.");
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
				throw protocolError("INVALID_PATH", "Artifact '" + artifactId + "' does not support fragment access.");
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
			structuredContent.put("isError", true);
			String message = e.getMessage() == null ? e.getClass().getName() : e.getMessage();
			structuredContent.put("message", message);
			ensureStaticReviewResource();
			return new EditArtifactResult(structuredContent, REVIEW_RESOURCE_URI, true, message);
		}
		Map<String, Object> update = new LinkedHashMap<String, Object>();
		update.put("artifactId", artifactId);
		update.put("path", path);
		if (preview) {
		}
		else {
			try {
				List<Validation<?>> validations = currentManager.updateFragment(currentArtifact, path, before, after);
				if (validations != null && !validations.isEmpty()) {
					update.put("validations", validationMaps(validations));
				}
				if (hasErrors(validations)) {
					update.put("error", buildValidationMessage(validations));
				}
				else {
					reloadArtifactAfterMcpUpdate(currentManager, artifactId, path);
				}
			}
			catch (Exception e) {
				update.put("error", e.getMessage() == null ? e.getClass().getName() : e.getMessage());
			}
		}
		Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
		structuredContent.put("path", path);
		structuredContent.put("mode", mode);
		structuredContent.put("original", before);
		structuredContent.put("new", after);
		structuredContent.put("diff", buildFallbackDiff(Arrays.asList(operation)));
		String message = update.get("error") == null ? null : update.get("error").toString();
		boolean isError = message != null;
		structuredContent.put("isError", isError);
		if (message != null) {
			structuredContent.put("message", message);
		}
		if (update.get("validations") != null) {
			structuredContent.put("validations", update.get("validations"));
		}
		ensureStaticReviewResource();
		return new EditArtifactResult(structuredContent, REVIEW_RESOURCE_URI, Boolean.valueOf(isError), message);
	}

	private void reloadArtifactAfterMcpUpdate(ArtifactFragmentManager<?> manager, String artifactId, String fragment) {
		if (manager.shouldReloadAfterChange(fragment)) {
			reloadArtifactAfterMcpUpdate(artifactId);
			notifyCollaborationReload(artifactId);
		}
	}

	private void reloadArtifactAfterMcpUpdate(String artifactId) {
		EAIResourceRepository.getInstance().reload(artifactId, false, false);
		reloadDependenciesAfterMcpUpdate(artifactId);
	}

	private void reloadArtifactAfterMcpCreate(String artifactId) {
		EAIResourceRepository.getInstance().reload(artifactId, false, true);
		reloadDependenciesAfterMcpUpdate(artifactId);
	}

	private void reloadDependenciesAfterMcpUpdate(final String artifactId) {
		Runnable reloadDependencies = new Runnable() {
			@Override
			public void run() {
				try {
					server.getRepository().reloadDependencies(Collections.singleton(artifactId));
				}
				catch (Exception e) {
					LOGGER.warn("Could not reload dependencies after MCP update for " + artifactId, e);
				}
			}
		};
		submitMcpReload(reloadDependencies, "Could not schedule dependency reload after MCP update for " + artifactId);
	}

	private void reloadArtifactsAfterMcpDelete(final List<String> artifactIds) {
		if (artifactIds == null || artifactIds.isEmpty()) {
			return;
		}
		Runnable reloadDependencies = new Runnable() {
			@Override
			public void run() {
				for (String artifactId : artifactIds) {
					try {
						server.getRepository().reload(artifactId, false);
					}
					catch (Exception e) {
						LOGGER.warn("Could not reload dependency after MCP delete for " + artifactId, e);
					}
				}
			}
		};
		submitMcpReload(reloadDependencies, "Could not schedule dependency reload after MCP delete");
	}

	private void submitMcpReload(Runnable reloadTask, String errorMessage) {
		try {
			if (server.getPool() != null) {
				server.getPool().submit(reloadTask);
			}
			else {
				ForkJoinPool.commonPool().submit(reloadTask);
			}
		}
		catch (RejectedExecutionException e) {
			LOGGER.warn(errorMessage, e);
		}
	}

	private void notifyCollaborationReload(String artifactId) {
		if (server.getCollaborationListener() != null) {
			server.getCollaborationListener().notifyArtifactReload(artifactId, "MCP updated");
		}
	}

	private void notifyCollaborationCreate(String artifactId) {
		if (server.getCollaborationListener() != null) {
			server.getCollaborationListener().notifyArtifactCreate(artifactId, "MCP created");
		}
	}

	private List<MCPFragmentSearchResult> search(MCPToolCallInput input, Map<String, Object> meta, MCPConfiguration configuration) {
		FragmentIndexService service = server.getFragmentIndexService();
		if (service == null) {
			throw new HTTPException(503, "The fragment index service is unavailable.");
		}
		String pattern = input == null ? null : input.getPattern();
		if (pattern == null || pattern.trim().isEmpty()) {
			throw new HTTPException(400, "Missing required argument 'pattern'.");
		}
		int before = number(input == null ? null : input.getBeforeContext());
		int after = number(input == null ? null : input.getAfterContext());
		int offset = number(input == null ? null : input.getOffset());
		if (input != null && input.getContext() != null) {
			before = input.getContext();
			after = input.getContext();
		}
		if (offset < 0) {
			throw new HTTPException(400, "Argument 'offset' must be a non-negative integer.");
		}
		List<String> namespaces = resolveNamespaces(configuration, input == null ? null : input.getNamespace(), meta);
		List<String> artifactTypes = input == null ? null : input.getArtifactType();
		List<String> artifactCategories = input == null ? null : input.getArtifactCategory();
		boolean caseSensitive = input != null && input.getCaseSensitive() != null ? input.getCaseSensitive().booleanValue() : false;
		List<FragmentSearch> search = service.search(pattern, input == null ? null : input.getGlob(), namespaces, artifactTypes, artifactCategories, caseSensitive, before, after, 0);
		List<MCPFragmentSearchResult> results = new ArrayList<MCPFragmentSearchResult>();
		for (FragmentSearch fragment : search) {
			results.add(new MCPFragmentSearchResult(fragment.getArtifactId(), fragment.getPath(), fragment.getArtifactType(), fragment.getArtifactCategory(), fragment.getFragmentType(), fragment.getContentType(), fragment.getProperties(), fragment.isEditable(), fragment.isRemovable(), groupMatches(fragment.getMatches())));
		}
		return results;
	}

	private Map<String, Object> optimizeResults(String pattern, List<MCPFragmentSearchResult> results, int totalResults, int offset, Integer limit, boolean pageTruncated) {
		int totalMatches = countMatches(results);
		Map<String, Object> structuredContent = searchStructuredContent(pattern, results, totalResults, totalMatches, "full", pageTruncated, offset, limit);
		if (structuredContentSize(structuredContent) <= MAX_RESULT_BYTES) {
			return structuredContent;
		}
		List<Map<String, Object>> reduced = reduceResults(results);
		structuredContent = searchStructuredContent(pattern, reduced, totalResults, totalMatches, "reduced", true, offset, limit);
		if (structuredContentSize(structuredContent) <= MAX_RESULT_BYTES) {
			return structuredContent;
		}
		List<Map<String, Object>> summary = summarizeResults(results);
		structuredContent = searchStructuredContent(pattern, summary, totalResults, totalMatches, "summary", true, offset, limit);
		if (structuredContentSize(structuredContent) <= MAX_RESULT_BYTES) {
			return structuredContent;
		}
		summary = reduceSummary(pattern, summary, totalResults, totalMatches, offset, limit);
		return searchStructuredContent(pattern, summary, totalResults, totalMatches, "summary", true, offset, limit);
	}

	private String buildEditSummaryText(Map<String, Object> structuredContent) {
		Object updatedCount = structuredContent.get("updatedCount");
		Object failedCount = structuredContent.get("failedCount");
		StringBuilder builder = new StringBuilder();
		builder.append("Updated ").append(updatedCount).append(" fragments, ").append(failedCount).append(" failed");
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> updates = (List<Map<String, Object>>) structuredContent.get("updates");
		if (updates != null) {
			for (Map<String, Object> update : updates) {
				if (update.get("error") != null) {
					builder.append("\n- ").append(update.get("path")).append(": ").append(update.get("error"));
				}
			}
		}
		return builder.toString();
	}

	private String buildWriteSummaryText(Map<String, Object> structuredContent) {
		StringBuilder builder = new StringBuilder();
		boolean isError = asBoolean(structuredContent.get("isError"));
		builder.append(isError ? "Failed to write " : "Wrote ").append(structuredContent.get("path"));
		if (structuredContent.get("message") != null) {
			builder.append("\n- ").append(structuredContent.get("message"));
		}
		return builder.toString();
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
		Number startLine = (Number) structuredContent.get("startLine");
		String path = (String) structuredContent.get("path");
		if ("EMPTY_RANGE".equals(structuredContent.get("code"))) {
			return "No lines returned from " + path + ": startLine " + startLine + " exceeds total " + total + ".";
		}
		return "Read " + count + " line(s) from " + path + " (start line " + startLine + ", total " + total + ").";
	}

	private boolean asBoolean(Object value) {
		return booleanArgument(value, false);
	}

	private boolean booleanArgument(Object value, boolean defaultValue) {
		Object unwrapped = unwrap(value);
		if (unwrapped == null) {
			return defaultValue;
		}
		if (unwrapped instanceof Boolean) {
			return ((Boolean) unwrapped).booleanValue();
		}
		if (unwrapped instanceof String) {
			String trimmed = ((String) unwrapped).trim();
			if ("true".equalsIgnoreCase(trimmed)) {
				return true;
			}
			if ("false".equalsIgnoreCase(trimmed)) {
				return false;
			}
		}
		throw protocolError("INVALID_BOOLEAN", "Expected a boolean value.");
	}

	private String buildSummaryText(Map<String, Object> structuredContent) {
		Number totalResults = (Number) structuredContent.get("totalResults");
		String mode = (String) structuredContent.get("mode");
		if (totalResults == null || totalResults.intValue() == 0) {
			return "No results found";
		}
		StringBuilder builder = new StringBuilder();
		builder.append("Found ").append(totalResults.intValue()).append(" results");
		if (!"full".equals(mode)) {
			builder.append(" (").append(mode).append(" output)");
		}
		if (Boolean.TRUE.equals(structuredContent.get("truncated"))) {
			builder.append(". Result set is too large; search for more specific terms or add narrower namespace, artifact type, category, or glob filters.");
		}
		return builder.toString();
	}

	private String buildSearchDisplayMessage(Map<String, Object> structuredContent) {
		Number totalResults = (Number) structuredContent.get("totalResults");
		if (totalResults == null || totalResults.intValue() == 0) {
			return "No artifact fragments matched the search.";
		}
		return "Found " + totalResults.intValue() + " matching artifact fragment" + (totalResults.intValue() == 1 ? "." : "s.");
	}

	private String buildDeleteDisplayMessage(Map<String, Object> structuredContent) {
		if (asBoolean(structuredContent.get("success"))) {
			return "Deleted artifact '" + string(structuredContent.get("artifactId")) + "'.";
		}
		String message = string(structuredContent.get("message"));
		return message == null ? "Artifact delete failed." : message;
	}

	private String buildMoveDisplayMessage(Map<String, Object> structuredContent) {
		if (asBoolean(structuredContent.get("success"))) {
			return "Moved entry to " + string(structuredContent.get("newId")) + ".";
		}
		String message = string(structuredContent.get("message"));
		return message == null ? "Artifact move failed." : message;
	}

	private String buildSkillsDisplayMessage(Map<String, Object> structuredContent) {
		Number count = (Number) structuredContent.get("count");
		int total = count == null ? 0 : count.intValue();
		return "Loaded guidance for " + total + " skill" + (total == 1 ? "." : "s.");
	}

	private String buildInvokeDisplayMessage(boolean isError, Map<String, Object> structuredContent) {
		String serviceId = string(structuredContent.get("serviceId"));
		return (isError ? "Service failed: " : "Service invoked: ") + serviceId;
	}

	private String buildTraceSearchDisplayMessage(Map<String, Object> structuredContent) {
		Number count = (Number) structuredContent.get("count");
		int total = count == null ? 0 : count.intValue();
		return "Trace search returned " + total + " result" + (total == 1 ? "." : "s.");
	}

	private String buildFindDisplayMessage(Map<String, Object> structuredContent) {
		Number count = (Number) structuredContent.get("count");
		int total = count == null ? 0 : count.intValue();
		if (total == 0) {
			return "No artifact fragments were found.";
		}
		return "Found " + total + " artifact fragment" + (total == 1 ? "." : "s.");
	}

	private String buildReadDisplayMessage(Map<String, Object> structuredContent) {
		String artifactId = string(structuredContent.get("artifactId"));
		String path = string(structuredContent.get("path"));
		if ("EMPTY_RANGE".equals(structuredContent.get("code"))) {
			return "No lines available in " + path + " for artifact " + artifactId + ".";
		}
		return "Read " + path + " from artifact " + artifactId + ".";
	}

	private String buildReadMultipleDisplayMessage(Map<String, Object> structuredContent) {
		int count = integer(structuredContent.get("count"), 0);
		int errorCount = integer(structuredContent.get("errorCount"), 0);
		boolean truncated = asBoolean(structuredContent.get("truncated"));
		String message = "Read " + count + " artifact fragment" + (count == 1 ? "" : "s");
		if (errorCount > 0) {
			message += "; missed " + errorCount + " fragment" + (errorCount == 1 ? "" : "s");
		}
		if (truncated) {
			message += " (truncated).";
		}
		else {
			message += ".";
		}
		return message;
	}

	private String buildCreateDisplayMessage(Map<String, Object> structuredContent) {
		String message = string(structuredContent.get("message"));
		if (message != null && !message.trim().isEmpty()) {
			return message;
		}
		String artifactId = string(structuredContent.get("artifactId"));
		return asBoolean(structuredContent.get("isError")) ? "Failed to create artifact." : "Created artifact '" + artifactId + "'.";
	}

	private String buildCreateProjectDisplayMessage(Map<String, Object> structuredContent) {
		String message = string(structuredContent.get("message"));
		if (message != null && !message.trim().isEmpty()) {
			return message;
		}
		String projectId = string(structuredContent.get("projectId"));
		return asBoolean(structuredContent.get("isError")) ? "Failed to create project." : "Created project '" + projectId + "'.";
	}

	private String buildEditDisplayMessage(Map<String, Object> structuredContent) {
		Object count = structuredContent.get("count");
		String path = string(structuredContent.get("path"));
		boolean isError = asBoolean(structuredContent.get("isError"));
		if (path != null) {
			return isError ? "Failed to update " + path + "." : "Updated " + path + ".";
		}
		return (isError ? "Failed to update " : "Updated ") + count + " artifact fragment" + ("1".equals(String.valueOf(count)) ? "." : "s.");
	}

	private String buildWriteDisplayMessage(Map<String, Object> structuredContent) {
		String path = string(structuredContent.get("path"));
		boolean isError = asBoolean(structuredContent.get("isError"));
		if (!isError) {
			return "Wrote " + path + ".";
		}
		return "Failed to write " + path + ".";
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
			List<String> matches = reduceContextLines(result.getMatches());
			if (matches.isEmpty()) {
				continue;
			}
			Map<String, Object> single = new LinkedHashMap<String, Object>();
			single.put("artifactId", result.getArtifactId());
			single.put("path", result.getPath());
			single.put("artifactType", result.getArtifactType());
			single.put("artifactCategory", result.getArtifactCategory());
			single.put("fragmentType", result.getFragmentType());
			single.put("contentType", result.getContentType());
			single.put("properties", result.getProperties());
			single.put("editable", result.isEditable());
			single.put("removable", result.isRemovable());
			single.put("matches", matches);
			reduced.add(single);
		}
		return reduced;
	}

	private List<String> reduceContextLines(List<String> matches) {
		if (matches == null || matches.isEmpty()) {
			return Collections.emptyList();
		}
		List<String> reduced = new ArrayList<String>();
		for (String chunk : matches) {
			StringBuilder builder = new StringBuilder();
			String[] lines = chunk.split("\\n");
			for (String line : lines) {
				if (isSearchMatchLine(line)) {
					if (builder.length() > 0) {
						builder.append('\n');
					}
					builder.append(line);
				}
			}
			if (builder.length() > 0) {
				reduced.add(builder.toString());
			}
		}
		return reduced;
	}

	private boolean isSearchMatchLine(String line) {
		if (line == null) {
			return false;
		}
		boolean sawDigit = false;
		for (int i = 0; i < line.length(); i++) {
			char character = line.charAt(i);
			if (character >= '0' && character <= '9') {
				sawDigit = true;
				continue;
			}
			return sawDigit && character == ':';
		}
		return false;
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

	private List<Map<String, Object>> reduceSummary(String pattern, List<Map<String, Object>> summary, int totalResults, int totalMatches, int offset, Integer limit) {
		Collections.sort(summary, new Comparator<Map<String, Object>>() {
			@Override
			public int compare(Map<String, Object> left, Map<String, Object> right) {
				int leftCount = ((Number) left.get("count")).intValue();
				int rightCount = ((Number) right.get("count")).intValue();
				return Integer.compare(rightCount, leftCount);
			}
		});
		int low = 0;
		int high = summary.size();
		int best = 0;
		while (low <= high) {
			int middle = low + (high - low) / 2;
			List<Map<String, Object>> candidate = new ArrayList<Map<String, Object>>(summary.subList(0, middle));
			Map<String, Object> structuredContent = searchStructuredContent(pattern, candidate, totalResults, totalMatches, "summary", true, offset, limit);
			if (structuredContentSize(structuredContent) <= MAX_RESULT_BYTES) {
				best = middle;
				low = middle + 1;
			}
			else {
				high = middle - 1;
			}
		}
		return new ArrayList<Map<String, Object>>(summary.subList(0, best));
	}

	private Map<String, Object> searchStructuredContent(String pattern, Object results, int totalResults, int totalMatches, String mode, boolean truncated, int offset, Integer limit) {
		Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
		structuredContent.put("results", results);
		structuredContent.put("pattern", pattern);
		structuredContent.put("count", results instanceof List ? ((List<?>) results).size() : 0);
		structuredContent.put("totalResults", totalResults);
		structuredContent.put("totalMatches", totalMatches);
		structuredContent.put("offset", offset);
		structuredContent.put("limit", limit);
		structuredContent.put("truncated", truncated);
		structuredContent.put("mode", mode);
		return structuredContent;
	}

	private int structuredContentSize(Map<String, Object> structuredContent) {
		try {
			return marshal(structuredContent).length;
		}
		catch (IOException e) {
			return Integer.MAX_VALUE;
		}
	}


	private int number(Integer value) {
		return value == null ? 0 : Math.max(0, value.intValue());
	}


	private String truncateToBytes(String input, int maxBytes) {
		if (input == null) {
			return null;
		}
		int bytes = 0;
		int end = 0;
		for (int i = 0; i < input.length();) {
			int codePoint = input.codePointAt(i);
			int codePointBytes = new String(Character.toChars(codePoint)).getBytes(StandardCharsets.UTF_8).length;
			if (bytes + codePointBytes > maxBytes) {
				break;
			}
			bytes += codePointBytes;
			end = i + Character.charCount(codePoint);
			i = end;
		}
		return end == input.length() ? input : input.substring(0, end);
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
			return right == null || right.isEmpty() ? null : new ArrayList<String>(right);
		}
		if (right == null || right.isEmpty()) {
			return new ArrayList<String>(left);
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

	@SuppressWarnings("unchecked")
	private List<Object> list(Object object) {
		Object unwrap = unwrap(object);
		return unwrap instanceof List ? (List<Object>) unwrap : null;
	}

	private String string(Object object) {
		Object unwrap = unwrap(object);
		return unwrap == null ? null : unwrap.toString();
	}

	private void ensureStaticReviewResource() {
		if (reviewResources().containsKey(REVIEW_RESOURCE_URI)) {
			return;
		}
		storeReviewResource(REVIEW_RESOURCE_URI, "diff.html", "text/html;profile=mcp-app", loadReviewResourceBytes("diff.html"));
	}

	private byte[] loadReviewResourceBytes(String name) {
		InputStream input = MCPREST.class.getClassLoader().getResourceAsStream(name);
		if (input == null) {
			throw new IllegalStateException("Missing review resource: " + name);
		}
		try {
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			byte[] buffer = new byte[4096];
			int read = 0;
			while ((read = input.read(buffer)) >= 0) {
				output.write(buffer, 0, read);
			}
			return output.toByteArray();
		}
		catch (IOException e) {
			throw new RuntimeException("Failed to load review resource: " + name, e);
		}
		finally {
			try {
				input.close();
			}
			catch (IOException e) {
				throw new RuntimeException("Failed to close review resource: " + name, e);
			}
		}
	}

	private String buildFallbackDiff(List<Map<String, Object>> operations) {
		StringBuilder builder = new StringBuilder();
		for (Map<String, Object> operation : operations) {
			builder.append("--- a/").append(operation.get("artifactId")).append("/").append(operation.get("path")).append("\n");
			builder.append("+++ b/").append(operation.get("artifactId")).append("/").append(operation.get("path")).append("\n");
			builder.append(buildLineDiff((String) operation.get("before"), (String) operation.get("after")));
		}
		return builder.toString();
	}

	static String buildLineDiff(String before, String after) {
		List<String> beforeLines = splitLines(before);
		List<String> afterLines = splitLines(after);
		List<DiffLine> lines = diffLines(beforeLines, afterLines);
		StringBuilder builder = new StringBuilder();
		int changeIndex = 0;
		while (changeIndex < lines.size()) {
			while (changeIndex < lines.size() && lines.get(changeIndex).prefix == ' ') {
				changeIndex++;
			}
			if (changeIndex >= lines.size()) {
				break;
			}
			int start = Math.max(0, changeIndex - 3);
			int lastChange = changeIndex;
			changeIndex++;
			while (changeIndex < lines.size()) {
				while (changeIndex < lines.size() && lines.get(changeIndex).prefix == ' ') {
					changeIndex++;
				}
				if (changeIndex >= lines.size() || changeIndex - lastChange > 6) {
					break;
				}
				lastChange = changeIndex;
				changeIndex++;
			}
			appendHunk(builder, lines, start, Math.min(lines.size(), lastChange + 4));
		}
		return builder.toString();
	}

	private static void appendHunk(StringBuilder builder, List<DiffLine> lines, int start, int end) {
		int beforeStart = findHunkStart(lines, start, true);
		int afterStart = findHunkStart(lines, start, false);
		int beforeCount = 0;
		int afterCount = 0;
		for (int i = start; i < end; i++) {
			DiffLine line = lines.get(i);
			if (line.prefix != '+') {
				beforeCount++;
			}
			if (line.prefix != '-') {
				afterCount++;
			}
		}
		builder.append("@@ -").append(hunkRange(beforeStart - 1, beforeCount)).append(" +").append(hunkRange(afterStart - 1, afterCount)).append(" @@\n");
		for (int i = start; i < end; i++) {
			DiffLine line = lines.get(i);
			builder.append(line.prefix).append(line.line).append("\n");
		}
	}

	private static List<DiffLine> diffLines(List<String> beforeLines, List<String> afterLines) {
		int[][] lengths = new int[beforeLines.size() + 1][afterLines.size() + 1];
		for (int i = beforeLines.size() - 1; i >= 0; i--) {
			for (int j = afterLines.size() - 1; j >= 0; j--) {
				if (beforeLines.get(i).equals(afterLines.get(j))) {
					lengths[i][j] = lengths[i + 1][j + 1] + 1;
				}
				else {
					lengths[i][j] = Math.max(lengths[i + 1][j], lengths[i][j + 1]);
				}
			}
		}
		List<DiffLine> lines = new ArrayList<DiffLine>();
		int beforeIndex = 0;
		int afterIndex = 0;
		while (beforeIndex < beforeLines.size() && afterIndex < afterLines.size()) {
			if (beforeLines.get(beforeIndex).equals(afterLines.get(afterIndex))) {
				lines.add(new DiffLine(' ', beforeLines.get(beforeIndex), beforeIndex + 1, afterIndex + 1));
				beforeIndex++;
				afterIndex++;
			}
			else if (lengths[beforeIndex + 1][afterIndex] >= lengths[beforeIndex][afterIndex + 1]) {
				lines.add(new DiffLine('-', beforeLines.get(beforeIndex), beforeIndex + 1, 0));
				beforeIndex++;
			}
			else {
				lines.add(new DiffLine('+', afterLines.get(afterIndex), 0, afterIndex + 1));
				afterIndex++;
			}
		}
		while (beforeIndex < beforeLines.size()) {
			lines.add(new DiffLine('-', beforeLines.get(beforeIndex), beforeIndex + 1, 0));
			beforeIndex++;
		}
		while (afterIndex < afterLines.size()) {
			lines.add(new DiffLine('+', afterLines.get(afterIndex), 0, afterIndex + 1));
			afterIndex++;
		}
		return lines;
	}

	private static int findHunkStart(List<DiffLine> lines, int start, boolean before) {
		for (int i = start; i < lines.size(); i++) {
			DiffLine line = lines.get(i);
			int lineNumber = before ? line.beforeLine : line.afterLine;
			if (lineNumber > 0) {
				return lineNumber;
			}
		}
		return 1;
	}

	private static String hunkRange(int start, int count) {
		int lineNumber = count == 0 ? start : start + 1;
		return lineNumber + "," + count;
	}

	private static List<String> splitLines(String content) {
		if (content == null || content.isEmpty()) {
			return Collections.emptyList();
		}
		List<String> lines = new ArrayList<String>(Arrays.asList(content.split("\\r?\\n", -1)));
		if (!lines.isEmpty() && lines.get(lines.size() - 1).isEmpty()) {
			lines.remove(lines.size() - 1);
		}
		return lines;
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
		text.put("text", textValue == null ? "" : textValue);
		content.add(text);
		return content;
	}

	private String toJson(Object value) {
		try {
			return new String(marshal(value), StandardCharsets.UTF_8);
		}
		catch (Exception e) {
			return String.valueOf(value);
		}
	}

	private Map<String, Object> buildToolDefinitionMeta(String resourceUri) {
		Map<String, Object> meta = new LinkedHashMap<String, Object>();
		if (resourceUri != null && !resourceUri.trim().isEmpty()) {
			Map<String, Object> ui = new LinkedHashMap<String, Object>();
			ui.put("resourceUri", resourceUri);
			meta.put("ui", ui);
		}
		return meta.isEmpty() ? null : meta;
	}

	private Map<String, Object> buildToolMeta(String resourceUri) {
		return buildToolMeta(resourceUri, null);
	}

	private Map<String, Object> buildToolMeta(String resourceUri, String displayMessage) {
		Map<String, Object> meta = new LinkedHashMap<String, Object>();
		if (displayMessage != null && !displayMessage.trim().isEmpty()) {
			meta.put("displayMessage", displayMessage);
		}
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

	private ToolResult errorToolResult(String toolName, Exception exception) {
		String message = firstExceptionMessage(exception);
		Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
		structuredContent.put("tool", toolName);
		structuredContent.put("details", buildExceptionMessageChain(exception));
		structuredContent.put("stacktrace", stacktrace(exception));
		return new ToolResult(
			structuredContent,
			textContent("Tool call failed: " + message),
			buildToolMeta(null, "Tool failed: " + toolName),
			true,
			message
		);
	}

	private void addCustomTools(List<Map<String, Object>> tools, MCPConfiguration configuration) {
		Set<String> toolNames = new LinkedHashSet<String>();
		for (String builtInToolName : BUILT_IN_TOOL_NAMES) {
			toolNames.add(builtInToolName);
		}
		for (Map<String, Object> tool : tools) {
			String name = string(tool.get("name"));
			if (name != null) {
				toolNames.add(name);
			}
		}
		for (MCPToolProvider<?> provider : customToolProviders()) {
			try {
				MCPToolDefinition definition = provider.getToolDefinition(new MCPToolDefinitionContext(server, configuration));
				if (definition == null || definition.getName() == null || definition.getName().trim().isEmpty() || toolNames.contains(definition.getName())) {
					continue;
				}
				tools.add(toolDefinitionMap(definition));
				toolNames.add(definition.getName());
			}
			catch (Throwable e) {
				LOGGER.warn("Could not load custom MCP tool definition from provider {}", provider.getClass().getName(), e);
			}
		}
	}

	private MCPToolProvider<?> findCustomToolProvider(String name, MCPConfiguration configuration) {
		if (name == null || name.trim().isEmpty()) {
			return null;
		}
		for (MCPToolProvider<?> provider : customToolProviders()) {
			try {
				MCPToolDefinition definition = provider.getToolDefinition(new MCPToolDefinitionContext(server, configuration));
				if (definition != null && name.equals(definition.getName())) {
					return provider;
				}
			}
			catch (Throwable e) {
				LOGGER.warn("Could not load custom MCP tool definition from provider {}", provider.getClass().getName(), e);
			}
		}
		return null;
	}

	private List<MCPToolProvider<?>> customToolProviders() {
		List<MCPToolProvider<?>> providers = new ArrayList<MCPToolProvider<?>>();
		try {
			for (MCPToolProvider<?> provider : ServiceLoader.load(MCPToolProvider.class)) {
				providers.add(provider);
			}
		}
		catch (Throwable e) {
			LOGGER.warn("Could not load custom MCP tool providers", e);
		}
		return providers;
	}

	private Map<String, Object> toolDefinitionMap(MCPToolDefinition definition) {
		Map<String, Object> tool = new LinkedHashMap<String, Object>();
		tool.put("name", definition.getName());
		if (definition.getTitle() != null) {
			tool.put("title", definition.getTitle());
		}
		if (definition.getDescription() != null) {
			tool.put("description", definition.getDescription());
		}
		if (definition.getAnnotations() != null && !definition.getAnnotations().isEmpty()) {
			Map<String, Object> annotations = new LinkedHashMap<String, Object>(definition.getAnnotations());
			if (definition.isPreviewSupported()) {
				annotations.put("preview", true);
			}
			tool.put("annotations", annotations);
		}
		else if (definition.isPreviewSupported()) {
			Map<String, Object> annotations = new LinkedHashMap<String, Object>();
			annotations.put("preview", true);
			tool.put("annotations", annotations);
		}
		if (definition.getInputSchema() != null) {
			tool.put("inputSchema", definition.getInputSchema());
		}
		if (definition.getOutputSchema() != null) {
			tool.put("outputSchema", definition.getOutputSchema());
		}
		if (definition.getMeta() != null && !definition.getMeta().isEmpty()) {
			tool.put("_meta", definition.getMeta());
		}
		return tool;
	}

	private boolean isBuiltInToolName(String name) {
		for (String builtInToolName : BUILT_IN_TOOL_NAMES) {
			if (builtInToolName.equals(name)) {
				return true;
			}
		}
		return false;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private ToolResult customToolResult(MCPToolProvider provider, Map<String, Object> arguments, HTTPRequest request, MCPConfiguration configuration, Map<String, Object> meta, boolean preview) throws Exception {
		Class<?> inputType = provider.getInputType();
		Object input = inputType == null || Map.class.isAssignableFrom(inputType)
			? arguments
			: bind(arguments, inputType);
		MCPToolResult result = provider.invoke(input, new MCPToolCallContext(server, request, resolveMcpToken(), header(request, MCP_SESSION_ID), configuration, meta, preview));
		if (result == null) {
			return new ToolResult(Collections.emptyMap(), textContent(""), null);
		}
		return new ToolResult(
			result.getStructuredContent(),
			textContent(result.getContent()),
			result.getMeta(),
			result.getIsError(),
			result.getMessage()
		);
	}

	private Token resolveMcpToken() {
		return server.isAnonymousIsRoot() ? SystemPrincipal.ROOT : null;
	}

	private void addDocumentationTools(List<Map<String, Object>> tools) {
		Map<String, Object> searchTool = new LinkedHashMap<String, Object>();
		searchTool.put("name", SEARCH_DOCUMENTATION_TOOL_NAME);
		searchTool.put("title", "Search nabu documentation");
		searchTool.put("description", "Search documentation files under protected/documentation. Match snippets include line numbers. Namespace identifies the documented repository entry; path is relative to namespace.");
		Map<String, Object> searchAnnotations = new LinkedHashMap<String, Object>();
		searchAnnotations.put("scopes", Arrays.asList("read:nabu:documentation"));
		searchAnnotations.put("intentTemplate", "Search documentation for {pattern} [in namespaces {namespace}] [with glob {glob}] [context {context}] [before {beforeContext}] [after {afterContext}]");
		searchTool.put("annotations", searchAnnotations);
		Map<String, Object> searchInputSchema = new LinkedHashMap<String, Object>();
		searchInputSchema.put("type", "object");
		Map<String, Object> searchProperties = new LinkedHashMap<String, Object>();
		searchProperties.put("pattern", schema("string"));
		Map<String, Object> glob = propertySchema("array", "Optional glob filters applied to documentation paths.");
		glob.put("items", schema("string"));
		searchProperties.put("glob", glob);
		Map<String, Object> namespace = propertySchema("array", "Optional namespace filters. Matches exact namespaces and descendants.");
		namespace.put("items", schema("string"));
		searchProperties.put("namespace", namespace);
		searchProperties.put("caseSensitive", propertySchema("boolean", "Whether matching is case-sensitive. Defaults to false when omitted."));
		searchProperties.put("beforeContext", schema("integer"));
		searchProperties.put("afterContext", schema("integer"));
		searchProperties.put("context", schema("integer"));
		searchProperties.put("limit", propertySchema("integer", "Maximum number of results to return (>0)."));
		searchProperties.put("offset", propertySchema("integer", "Number of matching results to skip before returning results. Default: 0."));
		searchInputSchema.put("properties", searchProperties);
		searchInputSchema.put("required", Arrays.asList("pattern"));
		searchTool.put("inputSchema", searchInputSchema);
		searchTool.put("outputSchema", searchOutputSchema());
		tools.add(searchTool);

		Map<String, Object> findTool = new LinkedHashMap<String, Object>();
		findTool.put("name", FIND_DOCUMENTATION_TOOL_NAME);
		findTool.put("title", "Find nabu documentation");
		findTool.put("description", "Find indexed documentation using namespace and documentation-relative path filters. The optional pattern is a full-match regex by default, not a contains search. For substring matching use .*text.* or set glob=true and use *text*. Results include editable/removable flags.");
		Map<String, Object> findAnnotations = new LinkedHashMap<String, Object>();
		findAnnotations.put("scopes", Arrays.asList("read:nabu:documentation"));
		findAnnotations.put("intentTemplate", "Find documentation [matching full regex/glob {pattern}] [in namespace {namespace}] [limit {limit}]");
		findTool.put("annotations", findAnnotations);
		Map<String, Object> findInputSchema = new LinkedHashMap<String, Object>();
		findInputSchema.put("type", "object");
		Map<String, Object> findProperties = new LinkedHashMap<String, Object>();
		findProperties.put("pattern", propertySchema("string", "Optional full-match regex applied to the namespace or documentation path, not a contains search. To match a substring use .*text.*. When glob=true, use glob syntax such as *text*."));
		findProperties.put("namespace", propertySchema("string", "Optional exact namespace filter."));
		findProperties.put("glob", propertySchema("boolean", "If true, interpret pattern as a full-match glob instead of a regex. For substring matching with glob use *text*."));
		findProperties.put("limit", propertySchema("integer", "Maximum number of results to return (>0)."));
		findProperties.put("offset", propertySchema("integer", "Number of matching results to skip before returning results."));
		findProperties.put("caseSensitive", propertySchema("boolean", "Whether matching is case-sensitive. Defaults to false when omitted."));
		findInputSchema.put("properties", findProperties);
		findTool.put("inputSchema", findInputSchema);
		findTool.put("outputSchema", findOutputSchema());
		tools.add(findTool);

		Map<String, Object> readTool = new LinkedHashMap<String, Object>();
		readTool.put("name", READ_DOCUMENTATION_TOOL_NAME);
		readTool.put("title", "Read nabu documentation");
		readTool.put("description", "Read lines from documentation. Returned content is raw text. Namespace identifies the documented repository entry; path is relative to the namespace.");
		Map<String, Object> readAnnotations = new LinkedHashMap<String, Object>();
		readAnnotations.put("scopes", Arrays.asList("read:nabu:documentation"));
		readAnnotations.put("intentTemplate", "Read documentation {namespace}/{path} [from line {startLine}] [limit {limit}]");
		readTool.put("annotations", readAnnotations);
		Map<String, Object> readInputSchema = documentationReadInputSchema(false);
		readTool.put("inputSchema", readInputSchema);
		readTool.put("outputSchema", readOutputSchema());
		tools.add(readTool);

		Map<String, Object> readMultipleTool = new LinkedHashMap<String, Object>();
		readMultipleTool.put("name", READ_MULTIPLE_DOCUMENTATION_TOOL_NAME);
		readMultipleTool.put("title", "Read multiple docs");
		readMultipleTool.put("description", "Read lines from multiple documentation files in one call. Returned content is raw text.");
		Map<String, Object> readMultipleAnnotations = new LinkedHashMap<String, Object>();
		readMultipleAnnotations.put("scopes", Arrays.asList("read:nabu:documentation"));
		readMultipleAnnotations.put("intentTemplate", "Read multiple documentation files");
		readMultipleTool.put("annotations", readMultipleAnnotations);
		Map<String, Object> readMultipleInputSchema = documentationReadInputSchema(true);
		readMultipleTool.put("inputSchema", readMultipleInputSchema);
		readMultipleTool.put("outputSchema", readMultipleOutputSchema());
		tools.add(readMultipleTool);

		Map<String, Object> editTool = new LinkedHashMap<String, Object>();
		editTool.put("name", EDIT_DOCUMENTATION_TOOL_NAME);
		editTool.put("title", "Edit nabu documentation");
		editTool.put("description", "Replace exact matches in editable documentation under protected/documentation.");
		Map<String, Object> editAnnotations = new LinkedHashMap<String, Object>();
		editAnnotations.put("scopes", Arrays.asList("write:nabu:documentation"));
		editAnnotations.put("intentTemplate", "Edit documentation {namespace}/{path} by replacing exact matches");
		editTool.put("annotations", editAnnotations);
		Map<String, Object> editInputSchema = new LinkedHashMap<String, Object>();
		editInputSchema.put("type", "object");
		Map<String, Object> editProperties = new LinkedHashMap<String, Object>();
		editProperties.put("namespace", propertySchema("string", "Namespace containing the documentation."));
		editProperties.put("path", propertySchema("string", "Path relative to protected/documentation."));
		editProperties.put("edits", editSchema());
		editInputSchema.put("properties", editProperties);
		editInputSchema.put("required", Arrays.asList("namespace", "path", "edits"));
		editTool.put("inputSchema", editInputSchema);
		editTool.put("outputSchema", editOutputSchema());
		editTool.put("_meta", buildToolDefinitionMeta(REVIEW_RESOURCE_URI));
		tools.add(editTool);

		Map<String, Object> writeTool = new LinkedHashMap<String, Object>();
		writeTool.put("name", WRITE_DOCUMENTATION_TOOL_NAME);
		writeTool.put("title", "Write nabu documentation");
		writeTool.put("description", "Overwrite, append, or prepend editable documentation under protected/documentation. Creates files when possible.");
		Map<String, Object> writeAnnotations = new LinkedHashMap<String, Object>();
		writeAnnotations.put("scopes", Arrays.asList("write:nabu:documentation"));
		writeAnnotations.put("intentTemplate", "Write documentation {namespace}/{path} [mode {mode}]");
		writeTool.put("annotations", writeAnnotations);
		Map<String, Object> writeInputSchema = new LinkedHashMap<String, Object>();
		writeInputSchema.put("type", "object");
		Map<String, Object> writeProperties = new LinkedHashMap<String, Object>();
		writeProperties.put("namespace", propertySchema("string", "Namespace containing the documentation."));
		writeProperties.put("path", propertySchema("string", "Path relative to protected/documentation."));
		writeProperties.put("content", propertySchema("string", "New documentation content."));
		Map<String, Object> mode = propertySchema("string", "Write mode. Default: overwrite.");
		mode.put("enum", Arrays.asList("overwrite", "append", "prepend"));
		writeProperties.put("mode", mode);
		writeInputSchema.put("properties", writeProperties);
		writeInputSchema.put("required", Arrays.asList("namespace", "path", "content"));
		writeTool.put("inputSchema", writeInputSchema);
		writeTool.put("outputSchema", writeOutputSchema());
		writeTool.put("_meta", buildToolDefinitionMeta(REVIEW_RESOURCE_URI));
		tools.add(writeTool);

		Map<String, Object> deleteTool = new LinkedHashMap<String, Object>();
		deleteTool.put("name", DELETE_DOCUMENTATION_TOOL_NAME);
		deleteTool.put("title", "Delete nabu documentation");
		deleteTool.put("description", "Delete removable documentation under protected/documentation.");
		Map<String, Object> deleteAnnotations = new LinkedHashMap<String, Object>();
		deleteAnnotations.put("scopes", Arrays.asList("write:nabu:documentation"));
		deleteAnnotations.put("intentTemplate", "Delete documentation {namespace}/{path}");
		deleteTool.put("annotations", deleteAnnotations);
		Map<String, Object> deleteInputSchema = new LinkedHashMap<String, Object>();
		deleteInputSchema.put("type", "object");
		Map<String, Object> deleteProperties = new LinkedHashMap<String, Object>();
		deleteProperties.put("namespace", propertySchema("string", "Namespace containing the documentation."));
		deleteProperties.put("path", propertySchema("string", "Path relative to protected/documentation."));
		deleteInputSchema.put("properties", deleteProperties);
		deleteInputSchema.put("required", Arrays.asList("namespace", "path"));
		deleteTool.put("inputSchema", deleteInputSchema);
		deleteTool.put("outputSchema", writeOutputSchema());
		deleteTool.put("_meta", buildToolDefinitionMeta(REVIEW_RESOURCE_URI));
		tools.add(deleteTool);
	}

	private Map<String, Object> documentationReadInputSchema(boolean multiple) {
		Map<String, Object> inputSchema = new LinkedHashMap<String, Object>();
		inputSchema.put("type", "object");
		Map<String, Object> properties = new LinkedHashMap<String, Object>();
		Map<String, Object> item = new LinkedHashMap<String, Object>();
		item.put("type", "object");
		Map<String, Object> itemProperties = new LinkedHashMap<String, Object>();
		itemProperties.put("namespace", propertySchema("string", "Namespace containing the documentation."));
		itemProperties.put("path", propertySchema("string", "Path relative to protected/documentation."));
		itemProperties.put("startLine", propertySchema("integer", "1-based line number to start reading from. Default: 1."));
		itemProperties.put("limit", propertySchema("integer", "Maximum number of lines to return (>0). Default: 200."));
		item.put("properties", itemProperties);
		item.put("required", Arrays.asList("namespace", "path"));
		if (multiple) {
			Map<String, Object> documents = schema("array");
			documents.put("items", item);
			properties.put("documents", documents);
			inputSchema.put("required", Arrays.asList("documents"));
		}
		else {
			properties.putAll(itemProperties);
			inputSchema.put("required", Arrays.asList("namespace", "path"));
		}
		inputSchema.put("properties", properties);
		return inputSchema;
	}

	private Map<String, Object> searchOutputSchema() {
		Map<String, Object> schema = new LinkedHashMap<String, Object>();
		schema.put("type", "object");
		Map<String, Object> properties = new LinkedHashMap<String, Object>();
		properties.put("results", schema("array"));
		properties.put("pattern", schema("string"));
		properties.put("count", schema("integer"));
		properties.put("totalResults", schema("integer"));
		properties.put("totalMatches", schema("integer"));
		properties.put("truncated", schema("boolean"));
		properties.put("mode", schema("string"));
		schema.put("properties", properties);
		return schema;
	}

	private Map<String, Object> editOutputSchema() {
		Map<String, Object> schema = new LinkedHashMap<String, Object>();
		schema.put("type", "object");
		Map<String, Object> properties = new LinkedHashMap<String, Object>();
		properties.put("path", schema("string"));
		properties.put("matchCount", schema("integer"));
		properties.put("original", schema("string"));
		properties.put("new", schema("string"));
		properties.put("diff", schema("string"));
		properties.put("count", schema("integer"));
		properties.put("updatedCount", schema("integer"));
		properties.put("failedCount", schema("integer"));
		properties.put("validations", schema("array"));
		Map<String, Object> updates = schema("array");
		Map<String, Object> updateItem = new LinkedHashMap<String, Object>();
		updateItem.put("type", "object");
		Map<String, Object> updateProperties = new LinkedHashMap<String, Object>();
		updateProperties.put("artifactId", schema("string"));
		updateProperties.put("path", schema("string"));
		updateProperties.put("matchCount", schema("integer"));
		updateProperties.put("original", schema("string"));
		updateProperties.put("new", schema("string"));
		updateProperties.put("diff", schema("string"));
		updateProperties.put("error", schema("string"));
		updateProperties.put("validations", schema("array"));
		updateItem.put("properties", updateProperties);
		updates.put("items", updateItem);
		properties.put("updates", updates);
		schema.put("properties", properties);
		return schema;
	}

	private Map<String, Object> findOutputSchema() {
		Map<String, Object> schema = new LinkedHashMap<String, Object>();
		schema.put("type", "object");
		Map<String, Object> properties = new LinkedHashMap<String, Object>();
		properties.put("files", schema("array"));
		properties.put("count", schema("integer"));
		properties.put("total", schema("integer"));
		properties.put("limit", schema("integer"));
		properties.put("offset", schema("integer"));
		properties.put("truncated", schema("boolean"));
		schema.put("properties", properties);
		return schema;
	}

	private Map<String, Object> readOutputSchema() {
		Map<String, Object> schema = new LinkedHashMap<String, Object>();
		schema.put("type", "object");
		Map<String, Object> properties = new LinkedHashMap<String, Object>();
		properties.put("artifactId", schema("string"));
		properties.put("path", schema("string"));
		properties.put("startLine", schema("integer"));
		properties.put("count", schema("integer"));
		properties.put("total", schema("integer"));
		properties.put("content", propertySchema("string", "Raw text content for the requested line range."));
		properties.put("truncated", schema("boolean"));
		Map<String, Object> truncatedReason = schema("array");
		truncatedReason.put("items", schema("string"));
		properties.put("truncated_reason", truncatedReason);
		properties.put("code", schema("string"));
		schema.put("properties", properties);
		return schema;
	}

	private Map<String, Object> readMultipleOutputSchema() {
		Map<String, Object> schema = new LinkedHashMap<String, Object>();
		schema.put("type", "object");
		Map<String, Object> properties = new LinkedHashMap<String, Object>();
		Map<String, Object> fragments = schema("array");
		fragments.put("items", readOutputSchema());
		properties.put("fragments", fragments);
		Map<String, Object> errors = schema("array");
		Map<String, Object> errorItem = new LinkedHashMap<String, Object>();
		errorItem.put("type", "object");
		Map<String, Object> errorProperties = new LinkedHashMap<String, Object>();
		errorProperties.put("artifactId", schema("string"));
		errorProperties.put("path", schema("string"));
		errorProperties.put("code", schema("string"));
		errorProperties.put("message", schema("string"));
		errorItem.put("properties", errorProperties);
		errors.put("items", errorItem);
		properties.put("errors", errors);
		properties.put("count", schema("integer"));
		properties.put("errorCount", schema("integer"));
		properties.put("totalRequested", schema("integer"));
		properties.put("totalBytes", schema("integer"));
		properties.put("truncated", schema("boolean"));
		schema.put("properties", properties);
		return schema;
	}

	private Map<String, Object> invokeOutputSchema() {
		Map<String, Object> schema = new LinkedHashMap<String, Object>();
		schema.put("type", "object");
		Map<String, Object> properties = new LinkedHashMap<String, Object>();
		properties.put("serviceId", schema("string"));
		properties.put("started", schema("string"));
		properties.put("stopped", schema("string"));
		properties.put("trace", schema("boolean"));
		properties.put("traceId", schema("string"));
		properties.put("exception", schema("string"));
		properties.put("output", new LinkedHashMap<String, Object>());
		schema.put("properties", properties);
		return schema;
	}

	private Map<String, Object> traceSearchOutputSchema() {
		Map<String, Object> schema = new LinkedHashMap<String, Object>();
		schema.put("type", "object");
		Map<String, Object> properties = new LinkedHashMap<String, Object>();
		properties.put("traceId", schema("string"));
		properties.put("count", schema("integer"));
		properties.put("truncated", schema("boolean"));
		properties.put("matches", schema("array"));
		schema.put("properties", properties);
		return schema;
	}

	private Map<String, Object> writeOutputSchema() {
		Map<String, Object> schema = new LinkedHashMap<String, Object>();
		schema.put("type", "object");
		Map<String, Object> properties = new LinkedHashMap<String, Object>();
		properties.put("path", schema("string"));
		properties.put("mode", schema("string"));
		properties.put("original", schema("string"));
		properties.put("new", schema("string"));
		properties.put("diff", schema("string"));
		properties.put("preview", schema("boolean"));
		properties.put("updated", schema("boolean"));
		properties.put("validations", schema("array"));
		schema.put("properties", properties);
		return schema;
	}

	private Map<String, Object> editSchema() {
		Map<String, Object> schema = new LinkedHashMap<String, Object>();
		schema.put("type", "array");
		schema.put("description", "List of exact find/replace edits to apply in order to a single artifact fragment. Each edit matches against the fragment state produced by the previous edit. Always use leading tabs instead of leading spaces in replacement content.");
		Map<String, Object> items = new LinkedHashMap<String, Object>();
		items.put("type", "object");
		Map<String, Object> properties = new LinkedHashMap<String, Object>();
		properties.put("find", propertySchema("string", "Exact text to find in the current fragment state (must match exactly once)."));
		properties.put("replace", propertySchema("string", "Replacement text."));
		items.put("properties", properties);
		items.put("required", Arrays.asList("find", "replace"));
		schema.put("items", items);
		return schema;
	}

	private Map<String, Object> propertySchema(String type, String description) {
		Map<String, Object> schema = schema(type);
		schema.put("description", description);
		return schema;
	}

	private ComplexContent bindServiceInput(DefinedService service, Map<String, Object> input) throws IOException, ParseException {
		if (service.getServiceInterface().getInputDefinition() == null) {
			return null;
		}
		if (input == null) {
			return service.getServiceInterface().getInputDefinition().newInstance();
		}
		return new MaskedContent(new MapContent(service.getServiceInterface().getInputDefinition(), input), service.getServiceInterface().getInputDefinition());
//		byte[] marshalled = marshal(input);
//		JSONBinding binding = new JSONBinding(service.getServiceInterface().getInputDefinition(), Charset.forName("UTF-8"));
//		binding.setEnableMapSupport(true);
//		binding.setAllowDynamicElements(true);
//		binding.setAddDynamicElementDefinitions(true);
//		return binding.unmarshal(IOUtils.toInputStream(IOUtils.wrap(marshalled, true)), new Window[0]);
	}

	private Token resolvePrincipal(Map<String, Object> arguments) {
		String runAs = string(arguments.get("runAs"));
		if (runAs == null || runAs.trim().isEmpty()) {
			return server.isAnonymousIsRoot() ? SystemPrincipal.ROOT : null;
		}
		String runAsRealm = string(arguments.get("runAsRealm"));
		if (runAsRealm == null || runAsRealm.trim().isEmpty()) {
			return server.isAnonymousIsRoot() ? SystemPrincipal.ROOT : null;
		}
		return new ImpersonateToken(null, runAsRealm.trim(), runAs.trim());
	}

	private String buildInvokeSummaryText(Map<String, Object> structuredContent) {
		String serviceId = string(structuredContent.get("serviceId"));
		boolean isError = asBoolean(structuredContent.get("isError"));
		StringBuilder builder = new StringBuilder();
		builder.append(isError ? "Invocation failed: " : "Invocation succeeded: ").append(serviceId);
		if (structuredContent.get("traceId") != null) {
			builder.append("\ntraceId: ").append(structuredContent.get("traceId"));
		}
		return builder.toString();
	}

	private String buildTraceSearchSummaryText(Map<String, Object> structuredContent) {
		return "Trace query count: " + structuredContent.get("count") + "\ntruncated: " + structuredContent.get("truncated");
	}

	private String buildInvokeFailureText(String serviceId, Throwable exception) {
		StringBuilder builder = new StringBuilder();
		builder.append("Service invocation failed for ").append(serviceId);
		builder.append("\n").append(buildExceptionMessageChain(exception));
		return builder.toString();
	}

	private Map<String, Object> searchTrace(String traceId, List<String> queries, int depth, int limit, int offset) throws IOException {
		try {
			javax.xml.parsers.DocumentBuilderFactory factory = javax.xml.parsers.DocumentBuilderFactory.newInstance();
			factory.setFeature(javax.xml.XMLConstants.FEATURE_SECURE_PROCESSING, true);
			factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
			factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
			factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
			factory.setXIncludeAware(false);
			factory.setExpandEntityReferences(false);
			org.w3c.dom.Document document = factory.newDocumentBuilder().parse(tracePath(traceId).toFile());
			javax.xml.xpath.XPath xpath = javax.xml.xpath.XPathFactory.newInstance().newXPath();
			List<Map<String, Object>> matches = new ArrayList<Map<String, Object>>();
			boolean truncated = false;
			for (String query : queries) {
				javax.xml.xpath.XPathExpression expression = xpath.compile(query);
				org.w3c.dom.NodeList nodes = (org.w3c.dom.NodeList) expression.evaluate(document, javax.xml.xpath.XPathConstants.NODESET);
				Map<String, Object> match = new LinkedHashMap<String, Object>();
				match.put("query", query);
				match.put("depth", depth);
				match.put("limit", limit);
				match.put("offset", offset);
				match.put("total", Integer.valueOf(nodes.getLength()));
				List<String> results = new ArrayList<String>();
				for (int i = offset; i < nodes.getLength() && results.size() < limit; i++) {
					org.w3c.dom.Node prepared = prepareSearchResultNode(nodes.item(i), depth);
					results.add(nodeToString(prepared));
				}
				match.put("results", results);
				matches.add(match);
			}
			String serialized = marshal(matches) == null ? "" : new String(marshal(matches), StandardCharsets.UTF_8);
			while (serialized.length() > 51200 && truncateMatches(matches)) {
				truncated = true;
				serialized = new String(marshal(matches), StandardCharsets.UTF_8);
			}
			Map<String, Object> structuredContent = new LinkedHashMap<String, Object>();
			structuredContent.put("traceId", traceId);
			structuredContent.put("count", matches.size());
			structuredContent.put("truncated", Boolean.valueOf(truncated));
			structuredContent.put("matches", matches);
			return structuredContent;
		}
		catch (Exception e) {
			throw new IOException(e);
		}
	}

	private org.w3c.dom.Node prepareSearchResultNode(org.w3c.dom.Node node, int depth) throws Exception {
		org.w3c.dom.Document copy = javax.xml.parsers.DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
		org.w3c.dom.Node imported = copy.importNode(node, true);
		if (imported.getNodeType() == org.w3c.dom.Node.ATTRIBUTE_NODE) {
			return imported;
		}
		if (imported.getNodeType() == org.w3c.dom.Node.ELEMENT_NODE) {
			copy.appendChild(imported);
			trimSearchResult(imported, depth, false);
			return imported;
		}
		org.w3c.dom.Element wrapper = copy.createElement("result");
		copy.appendChild(wrapper);
		wrapper.appendChild(imported);
		return imported;
	}

	private boolean trimSearchResult(org.w3c.dom.Node node, int depth, boolean insideInvoke) {
		if (!(node instanceof org.w3c.dom.Element)) {
			return false;
		}
		org.w3c.dom.Element element = (org.w3c.dom.Element) node;
		String name = element.getTagName();
		boolean isInvoke = "invoke".equals(name);
		boolean currentInsideInvoke = insideInvoke || isInvoke;
		if (("input".equals(name) || "output".equals(name)) && currentInsideInvoke) {
			element.setTextContent("");
			element.setAttribute("truncated", "true");
			return false;
		}
		org.w3c.dom.NodeList children = element.getChildNodes();
		List<org.w3c.dom.Node> toRemove = new ArrayList<org.w3c.dom.Node>();
		boolean hasInvokePath = isInvoke;
		for (int i = 0; i < children.getLength(); i++) {
			org.w3c.dom.Node child = children.item(i);
			if (child instanceof org.w3c.dom.Element) {
				org.w3c.dom.Element childElement = (org.w3c.dom.Element) child;
				boolean childInvoke = "invoke".equals(childElement.getTagName());
				if (childInvoke && depth <= 0) {
					stripElement(childElement);
					childElement.setAttribute("truncated", "true");
					hasInvokePath = true;
				}
				else {
					boolean childHasInvokePath = trimSearchResult(child, childInvoke ? depth - 1 : depth, currentInsideInvoke);
					if (!childHasInvokePath && !childInvoke) {
						toRemove.add(child);
					}
					else {
						hasInvokePath = true;
					}
				}
			}
		}
		for (org.w3c.dom.Node child : toRemove) {
			element.removeChild(child);
		}
		if (!toRemove.isEmpty()) {
			element.setAttribute("truncated", "true");
		}
		return hasInvokePath || !currentInsideInvoke;
	}

	private boolean truncateMatches(List<Map<String, Object>> matches) throws IOException {
		for (Map<String, Object> match : matches) {
			@SuppressWarnings("unchecked")
			List<String> results = (List<String>) match.get("results");
			if (results == null) {
				continue;
			}
			for (int i = 0; i < results.size(); i++) {
				String truncated = truncateSerializedXml(results.get(i));
				if (!truncated.equals(results.get(i))) {
					results.set(i, truncated);
					return true;
				}
			}
		}
		return false;
	}

	private String truncateSerializedXml(String xml) throws IOException {
		try {
			javax.xml.parsers.DocumentBuilderFactory factory = javax.xml.parsers.DocumentBuilderFactory.newInstance();
			factory.setFeature(javax.xml.XMLConstants.FEATURE_SECURE_PROCESSING, true);
			factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
			factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
			factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
			factory.setXIncludeAware(false);
			factory.setExpandEntityReferences(false);
			org.w3c.dom.Document document = factory.newDocumentBuilder().parse(new org.xml.sax.InputSource(new java.io.StringReader(xml)));
			if (truncateElement(document.getDocumentElement())) {
				return nodeToString(document.getDocumentElement());
			}
			return xml;
		}
		catch (Exception e) {
			throw new IOException(e);
		}
	}

	private boolean truncateElement(org.w3c.dom.Element element) {
		org.w3c.dom.NodeList children = element.getChildNodes();
		for (int i = children.getLength() - 1; i >= 0; i--) {
			org.w3c.dom.Node child = children.item(i);
			if (!(child instanceof org.w3c.dom.Element)) {
				continue;
			}
			org.w3c.dom.Element childElement = (org.w3c.dom.Element) child;
			if ("invoke".equals(childElement.getTagName())) {
				if (truncateElement(childElement)) {
					element.setAttribute("truncated", "true");
					return true;
				}
				continue;
			}
			stripElement(childElement);
			childElement.setAttribute("truncated", "true");
			element.setAttribute("truncated", "true");
			return true;
		}
		if ("input".equals(element.getTagName()) || "output".equals(element.getTagName())) {
			element.setTextContent("");
			element.setAttribute("truncated", "true");
			return true;
		}
		return false;
	}

	private void stripElement(org.w3c.dom.Element element) {
		while (element.hasChildNodes()) {
			element.removeChild(element.getFirstChild());
		}
	}

	private String nodeToString(org.w3c.dom.Node node) throws Exception {
		if (node == null) {
			return "";
		}
		short nodeType = node.getNodeType();
		if (nodeType == org.w3c.dom.Node.ATTRIBUTE_NODE) {
			return "@" + node.getNodeName() + "=\"" + escapeXml(node.getNodeValue()) + "\"";
		}
		if (nodeType == org.w3c.dom.Node.TEXT_NODE || nodeType == org.w3c.dom.Node.CDATA_SECTION_NODE) {
			return escapeXml(node.getNodeValue());
		}
		javax.xml.transform.TransformerFactory factory = javax.xml.transform.TransformerFactory.newInstance();
		try {
			factory.setAttribute("indent-number", Integer.valueOf(1));
		}
		catch (IllegalArgumentException e) {
			LOGGER.debug("TransformerFactory does not support indent-number", e);
		}
		javax.xml.transform.Transformer transformer = factory.newTransformer();
		transformer.setOutputProperty(javax.xml.transform.OutputKeys.OMIT_XML_DECLARATION, "yes");
		transformer.setOutputProperty(javax.xml.transform.OutputKeys.INDENT, "yes");
		transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "1");
		java.io.StringWriter writer = new java.io.StringWriter();
		transformer.transform(new javax.xml.transform.dom.DOMSource(node), new javax.xml.transform.stream.StreamResult(writer));
		return writer.toString().replace("    ", "\t").trim();
	}

	private String escapeXml(String value) {
		if (value == null || value.isEmpty()) {
			return "";
		}
		return value
			.replace("&", "&amp;")
			.replace("<", "&lt;")
			.replace(">", "&gt;")
			.replace("\"", "&quot;")
			.replace("'", "&apos;");
	}

	private java.nio.file.Path traceRoot() throws IOException {
		java.nio.file.Path path = MCPUtils.getTracesPath();
		Files.createDirectories(path);
		return path;
	}

	private java.nio.file.Path tracePath(String traceId) throws IOException {
		java.nio.file.Path root = traceRoot();
		try (java.util.stream.Stream<java.nio.file.Path> stream = Files.list(root)) {
			java.nio.file.Path match = stream.filter(single -> single.getFileName().toString().endsWith("-" + traceId + ".xml")).findFirst().orElse(null);
			if (match == null) {
				throw protocolError("UNKNOWN_TRACE_ID", "Trace not found for traceId '" + traceId + "'.");
			}
			return match;
		}
	}

	private List<Map<String, String>> extractEdits(Map<String, Object> arguments, String defaultArtifactId, String defaultPath) {
		if (arguments == null || !arguments.containsKey("edits")) {
			throw protocolError("MISSING_EDITS", "Missing required argument 'edits'.");
		}
		Object rawEdits = unwrap(arguments.get("edits"));
		if (!(rawEdits instanceof List)) {
			throw protocolError("INVALID_EDITS", "Argument 'edits' must be an array.");
		}
		List<?> values = (List<?>) rawEdits;
		if (values.isEmpty()) {
			throw protocolError("EMPTY_EDITS", "Argument 'edits' must contain at least one edit.");
		}
		List<Map<String, String>> edits = new ArrayList<Map<String, String>>();
		for (int i = 0; i < values.size(); i++) {
			if (!(values.get(i) instanceof Map)) {
				throw protocolError("INVALID_EDITS", "Entry " + i + " in 'edits' must be an object.");
			}
			Map<String, Object> edit = map(values.get(i));
			String artifactId = string(edit.get("artifactId"));
			if (artifactId == null) {
				artifactId = defaultArtifactId;
			}
			if (artifactId == null) {
				throw protocolError("MISSING_ARTIFACT_ID", "Missing required argument 'artifactId'.");
			}
			String path = string(edit.get("path"));
			if (path == null) {
				path = defaultPath;
			}
			if (path == null) {
				throw protocolError("MISSING_PATH", "Missing required argument 'path'.");
			}
			String find = requiredString(edit, "find", "MISSING_FIND");
			String replace = requiredString(edit, "replace", "MISSING_REPLACE");
			if (find.isEmpty()) {
				throw protocolError("FIND_EMPTY", "Entry " + i + " in 'edits' has an empty 'find' value.");
			}
			Map<String, String> normalized = new LinkedHashMap<String, String>();
			normalized.put("artifactId", artifactId);
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
			throw protocolError(code, "Missing required argument '" + key + "'.");
		}
		return value;
	}

	private static String stacktrace(Throwable throwable) {
		StringWriter writer = new StringWriter();
		PrintWriter printer = new PrintWriter(writer);
		throwable.printStackTrace(printer);
		printer.flush();
		return writer.toString();
	}

	private String firstExceptionMessage(Throwable throwable) {
		if (throwable == null) {
			return "No exception details available.";
		}
		String message = throwable.getMessage();
		if (message != null && !message.trim().isEmpty()) {
			return message.trim();
		}
		return throwable.getClass().getName();
	}

	private String buildExceptionMessageChain(Throwable throwable) {
		if (throwable == null) {
			return "No exception details available.";
		}
		StringBuilder builder = new StringBuilder();
		Throwable current = throwable;
		boolean first = true;
		while (current != null) {
			if (!first) {
				builder.append("\nCaused by: ");
			}
			builder.append(exceptionMessage(current));
			Throwable cause = current.getCause();
			if (cause == current) {
				break;
			}
			current = cause;
			first = false;
		}
		return builder.toString();
	}

	private String exceptionMessage(Throwable throwable) {
		String message = throwable.getMessage();
		return throwable.getClass().getName() + (message == null || message.trim().isEmpty() ? "" : ": " + message.trim());
	}

	private ArtifactFragment findEditableFragment(ArtifactFragmentManager<Artifact> manager, Artifact artifact, String artifactId, String path) {
		List<ArtifactFragment> fragments = manager.listFragments(artifact);
		if (fragments != null) {
			for (ArtifactFragment fragment : fragments) {
				if (fragment != null && path.equals(fragment.getPath())) {
					if (!fragment.isEditable()) {
						throw protocolError("INVALID_PATH", "Fragment '" + path + "' in artifact '" + artifactId + "' is not editable.");
					}
					return fragment;
				}
			}
		}
		throw protocolError("INVALID_PATH", "Fragment not found for artifact '" + artifactId + "' at path '" + path + "'.");
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

	private boolean matchesFindPattern(String pattern, boolean glob, boolean caseSensitive, String artifactId, String path) {
		String candidate = artifactId + "/" + path;
		String normalizedPattern = glob ? globToRegex(pattern) : pattern;
		if (!caseSensitive) {
			normalizedPattern = "(?i)" + normalizedPattern;
		}
		try {
			return candidate.matches(normalizedPattern) || path.matches(normalizedPattern) || artifactId.matches(normalizedPattern);
		}
		catch (Exception e) {
			throw protocolError("INVALID_PATTERN", "Invalid search pattern: '" + pattern + "'.");
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

	private List<FragmentSearch> getIndexedFragments(List<String> artifactIds, List<String> paths, List<String> namespaces) {
		FragmentIndexService service = server.getFragmentIndexService();
		if (service == null) {
			throw new HTTPException(503, "The fragment index service is unavailable.");
		}
		List<FragmentSearch> fragments = service.get(artifactIds, paths);
		List<FragmentSearch> allowed = new ArrayList<FragmentSearch>();
		for (FragmentSearch fragment : fragments) {
			if (matchesNamespace(namespaces, fragment.getArtifactId())) {
				allowed.add(fragment);
			}
		}
		return allowed;
	}

	private FragmentSearch getIndexedFragment(String artifactId, String path, List<String> namespaces) {
		List<FragmentSearch> fragments = getIndexedFragments(Collections.singletonList(artifactId), Collections.singletonList(path), namespaces);
		for (FragmentSearch fragment : fragments) {
			if (artifactId.equals(fragment.getArtifactId()) && path.equals(fragment.getPath())) {
				return fragment;
			}
		}
		throw protocolError("INVALID_PATH", "Fragment not found for artifact '" + artifactId + "' at path '" + path + "'.");
	}

	private Map<String, FragmentSearch> indexFragments(List<FragmentSearch> fragments) {
		Map<String, FragmentSearch> indexed = new LinkedHashMap<String, FragmentSearch>();
		for (FragmentSearch fragment : fragments) {
			indexed.put(fragmentKey(fragment.getArtifactId(), fragment.getPath()), fragment);
		}
		return indexed;
	}

	private String fragmentKey(String artifactId, String path) {
		return artifactId + "\n" + path;
	}

	private boolean matchesNamespace(List<String> namespaces, String artifactId) {
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
		throw protocolError("INVALID_NUMBER", "Expected an integer value.");
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

	private static class TraceRun implements Closeable {
		private final String traceId;
		private final java.nio.file.Path path;
		private final MCPInvokeTraceTracker tracker;

		private TraceRun(String traceId, java.nio.file.Path path, MCPInvokeTraceTracker tracker) {
			this.traceId = traceId;
			this.path = path;
			this.tracker = tracker;
		}

		private static TraceRun start(be.nabu.eai.repository.api.Repository repository, DefinedService service) throws IOException {
			String traceId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
			java.nio.file.Path traceRoot = MCPUtils.getTracesPath();
			Files.createDirectories(traceRoot);
			java.nio.file.Path path = traceRoot.resolve(service.getId() + "-" + traceId + ".xml");
			BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
			return new TraceRun(traceId, path, new MCPInvokeTraceTracker(repository, service.getId(), traceId, writer, path));
		}

		@Override
		public void close() throws IOException {
			tracker.close();
		}
	}

	private static class MCPInvokeTraceTracker implements ServiceRuntimeTracker, Closeable {
		private final be.nabu.eai.repository.api.Repository repository;
		private final String rootServiceId;
		private final String traceId;
		private final BufferedWriter writer;
		private final java.nio.file.Path path;
		private final Deque<String> elementStack = new ArrayDeque<String>();
		private final Deque<Object> hookStack = new ArrayDeque<Object>();
		private final Deque<Boolean> errorStack = new ArrayDeque<Boolean>();
		private boolean closed;

		private MCPInvokeTraceTracker(be.nabu.eai.repository.api.Repository repository, String rootServiceId, String traceId, BufferedWriter writer, java.nio.file.Path path) {
			this.repository = repository;
			this.rootServiceId = rootServiceId;
			this.traceId = traceId;
			this.writer = writer;
			this.path = path;
		}

		@Override
		public void describe(Object object) {
			// descriptions are written as static step attributes when available
		}

		@Override
		public void report(Object object) {
			// ignore for now
		}

		@Override
		public void start(Service service) {
			DefinedService defined = service instanceof DefinedService ? (DefinedService) service : null;
			String serviceId = defined == null ? rootServiceId : defined.getId();
			try {
				writeIndent();
				boolean rootInvoke = elementStack.isEmpty();
				writer.write("<invoke serviceId=\"" + escape(serviceId) + "\""
					+ (rootInvoke ? " traceId=\"" + traceId + "\"" : "")
					+ " started=\"" + TRACE_TIME_FORMATTER.format(Instant.now()) + "\">\n");
				elementStack.push("invoke");
				errorStack.push(false);
				hookStack.push(service);
				writeSerialized("input", ServiceRuntime.getRuntime() == null ? null : ServiceRuntime.getRuntime().getInput());
				be.nabu.eai.repository.api.MCPTraceProvider provider = resolveProvider(defined == null ? null : defined.getClass());
				if (provider != null) {
					provider.start(new ProviderTraceContext(writer, traceId, rootServiceId, elementStack.size()), service);
				}
				writer.flush();
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void stop(Service service) {
			try {
				be.nabu.eai.repository.api.MCPTraceProvider provider = resolveProvider(service == null ? null : service.getClass());
				if (provider != null) {
					provider.stop(new ProviderTraceContext(writer, traceId, rootServiceId, elementStack.size()), service);
				}
				writeSerialized("output", ServiceRuntime.getRuntime() == null ? null : ServiceRuntime.getRuntime().getOutput());
				writeClose("invoke");
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void error(Service service, Exception exception) {
			try {
				writeError(exception);
				be.nabu.eai.repository.api.MCPTraceProvider provider = resolveProvider(service == null ? null : service.getClass());
				if (provider != null) {
					provider.error(new ProviderTraceContext(writer, traceId, rootServiceId, elementStack.size()), service, exception);
				}
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void before(Object step) {
			try {
				be.nabu.eai.repository.api.MCPTraceProvider provider = resolveProvider(step == null ? null : step.getClass());
				if (provider != null) {
					provider.before(new ProviderTraceContext(writer, traceId, rootServiceId, elementStack.size()), step);
				}
				hookStack.push(step);
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void after(Object step) {
			try {
				be.nabu.eai.repository.api.MCPTraceProvider provider = resolveProvider(step == null ? null : step.getClass());
				if (provider != null) {
					provider.after(new ProviderTraceContext(writer, traceId, rootServiceId, elementStack.size()), step);
				}
				if (!hookStack.isEmpty()) {
					hookStack.pop();
				}
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void error(Object step, Exception exception) {
			try {
				writeError(exception);
				be.nabu.eai.repository.api.MCPTraceProvider provider = resolveProvider(step == null ? null : step.getClass());
				if (provider != null) {
					provider.error(new ProviderTraceContext(writer, traceId, rootServiceId, elementStack.size()), step, exception);
				}
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		private be.nabu.eai.repository.api.MCPTraceProvider resolveProvider(Class<?> type) {
			if (type == null) {
				return null;
			}
			if (TRACE_PROVIDER_CACHE.containsKey(type)) {
				return TRACE_PROVIDER_CACHE.get(type);
			}
			be.nabu.eai.repository.api.MCPTraceProvider closest = null;
			Class<?> closestType = null;
			for (be.nabu.eai.repository.api.MCPTraceProvider provider : repository.getArtifacts(be.nabu.eai.repository.api.MCPTraceProvider.class)) {
				try {
					java.lang.reflect.Method method = provider.getClass().getMethod("getArtifactClass");
					Class<?> artifactClass = (Class<?>) method.invoke(provider);
					if (artifactClass != null && artifactClass.isAssignableFrom(type)) {
						if (closest == null || closestType.isAssignableFrom(artifactClass)) {
							closest = provider;
							closestType = artifactClass;
						}
					}
				}
				catch (Exception e) {
					// ignore invalid providers
				}
			}
			if (closest != null) {
				TRACE_PROVIDER_CACHE.putIfAbsent(type, closest);
			}
			return closest;
		}

		private void writeSerialized(String tag, ComplexContent content) throws IOException {
			if (content == null) {
				return;
			}
			writeIndent();
			writer.write("\t<" + tag + ">\n");
			String serialized = serialize(content);
			if (serialized != null && !serialized.trim().isEmpty()) {
				for (String line : serialized.trim().split("\\r?\\n")) {
					if (line.trim().isEmpty()) {
						continue;
					}
					writeIndent();
					writer.write("\t\t" + line + "\n");
				}
			}
			writeIndent();
			writer.write("\t</" + tag + ">\n");
		}

		private String serialize(ComplexContent content) {
			try {
				XMLBinding binding = new XMLBinding(content.getType(), Charset.forName("UTF-8"));
				binding.setPrettyPrint(true);
				// Disable xsi output to reduce token size from repeated namespace declarations.
				binding.setAllowXSI(false);
				binding.setNamespaceAware(false);
				ByteArrayOutputStream output = new ByteArrayOutputStream();
				binding.marshal(output, new StreamHiderContent(content));
				return new String(output.toByteArray(), StandardCharsets.UTF_8).trim();
			}
			catch (Exception e) {
				return "<error>" + escape(stacktrace(e)) + "</error>";
			}
		}

		private void writeError(Exception exception) throws IOException {
			boolean handled = false;
			String content = stacktrace(exception);
			if (exception instanceof ServiceException) {
				ServiceException serviceException = (ServiceException) exception;
				handled = serviceException.isReported();
				if (handled) {
					content = "see original error";
				}
				else {
					serviceException.setReported(true);
				}
			}
			if (!errorStack.isEmpty() && errorStack.peek()) {
				return;
			}
			writeIndent();
			writer.write("\t<error handled=\"" + handled + "\">" + escape(content) + "</error>\n");
			writer.flush();
			if (!errorStack.isEmpty()) {
				errorStack.pop();
				errorStack.push(true);
			}
		}

		private void writeClose(String tag) throws IOException {
			if (!elementStack.isEmpty()) {
				elementStack.pop();
			}
			if (!errorStack.isEmpty()) {
				errorStack.pop();
			}
			writeIndent();
			writer.write("</" + tag + ">\n");
			writer.flush();
		}

		private void writeIndent() throws IOException {
			for (int i = 0; i < elementStack.size(); i++) {
				writer.write("\t");
			}
		}

		@Override
		public void close() throws IOException {
			if (!closed) {
				closed = true;
				while (!elementStack.isEmpty()) {
					writeClose(elementStack.peek());
				}
				writer.close();
			}
		}
	}

	private static class ProviderTraceContext implements be.nabu.eai.repository.api.MCPTraceContext {
		private final Writer writer;
		private final String traceId;
		private final String rootServiceId;
		private final int depth;
		private final Map<String, Object> attributes = new LinkedHashMap<String, Object>();

		private ProviderTraceContext(Writer writer, String traceId, String rootServiceId, int depth) {
			this.writer = writer;
			this.traceId = traceId;
			this.rootServiceId = rootServiceId;
			this.depth = depth;
		}

		@Override
		public String getTraceId() {
			return traceId;
		}

		@Override
		public String getRootServiceId() {
			return rootServiceId;
		}

		@Override
		public Writer getWriter() {
			return writer;
		}

		@Override
		public Map<String, Object> getAttributes() {
			return attributes;
		}

		@Override
		public boolean isIncludeLinks() {
			return Boolean.parseBoolean(System.getProperty(TRACE_INCLUDE_LINKS, "false"));
		}

		@Override
		public int getDepth() {
			return depth;
		}

		@Override
		public void write(String value) throws IOException {
			writer.write(value);
		}

		@Override
		public void writeLine(String value) throws IOException {
			for (int i = 0; i < depth; i++) {
				writer.write("\t");
			}
			writer.write(value);
			writer.write("\n");
		}

		@Override
		public void flush() throws IOException {
			writer.flush();
		}
	}

	private static String escape(String value) {
		if (value == null) {
			return null;
		}
		return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
	}
}
