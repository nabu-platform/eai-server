package be.nabu.eai.server.rest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.mime.api.Header;
import be.nabu.utils.mime.impl.MimeHeader;
import be.nabu.utils.mime.impl.PlainMimeContentPart;

@Path("/mcp")
public class MCPREST {

	private static final String MCP_VERSION = "2025-03-26";
	private static final String TOOL_NAME = "search_nabu_artifacts";
	private static final String MCP_SESSION_ID = "MCP-Session-Id";
	private static final String SESSION_MAP = "mcp.rest.sessions";
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
			result.put("capabilities", capabilities);
			Map<String, Object> serverInfo = new LinkedHashMap<String, Object>();
			serverInfo.put("name", "nabu-mcp");
			serverInfo.put("version", "1.0");
			result.put("serverInfo", serverInfo);
			result.put("configSchema", configSchema());
			response.put("result", result);
			return json(response, sessionId);
		}
		if ("tools/list".equals(method)) {
			resolveSessionConfiguration(request, false);
			Map<String, Object> result = new LinkedHashMap<String, Object>();
			Map<String, Object> tool = new LinkedHashMap<String, Object>();
			tool.put("name", TOOL_NAME);
			tool.put("title", "Search nabu artifacts");
			tool.put("description", "Search indexed artifact fragments ripgrep style. Namespace filters artifacts by id prefix, while glob only filters fragment paths.");
			Map<String, Object> inputSchema = new LinkedHashMap<String, Object>();
			inputSchema.put("type", "object");
			Map<String, Object> properties = new LinkedHashMap<String, Object>();
			properties.put("pattern", schema("string"));
			Map<String, Object> glob = schema("array");
			glob.put("items", schema("string"));
			glob.put("description", "Optional ripgrep-style glob filters applied only to fragment paths, not artifact ids.");
			properties.put("glob", glob);
			Map<String, Object> namespace = schema("array");
			namespace.put("items", schema("string"));
			namespace.put("description", "Optional artifact namespace filters. Matches the exact namespace and all descendant artifact ids. Configured and policy namespaces are applied first; this argument can only narrow further.");
			properties.put("namespace", namespace);
			properties.put("case_sensitive", schema("string"));
			properties.put("before_context", schema("integer"));
			properties.put("after_context", schema("integer"));
			properties.put("context", schema("integer"));
			inputSchema.put("properties", properties);
			inputSchema.put("required", Arrays.asList("pattern"));
			tool.put("inputSchema", inputSchema);
			result.put("tools", Arrays.asList(tool));
			response.put("result", result);
			return json(response, null);
		}
		if ("tools/call".equals(method)) {
			MCPConfiguration configuration = resolveSessionConfiguration(request, true);
			Map<String, Object> params = map(rpc.get("params"));
			String name = params == null ? null : string(params.get("name"));
			if (!TOOL_NAME.equals(name)) {
				response.put("error", error(-32602, "Unknown tool: " + name));
				return json(response, null);
			}
			Map<String, Object> arguments = map(params.get("arguments"));
			Map<String, Object> meta = map(params.get("_meta"));
			MCPToolCallInput input = bind(arguments, MCPToolCallInput.class);
			Map<String, Object> result = new LinkedHashMap<String, Object>();
			List<MCPFragmentSearchResult> results = search(input, meta, configuration);
			Map<String, Object> structuredContent = optimizeResults(input.getPattern(), results);
			List<Map<String, String>> contentResult = new ArrayList<Map<String, String>>();
			Map<String, String> text = new LinkedHashMap<String, String>();
			text.put("type", "text");
			text.put("text", buildSummaryText(structuredContent));
			contentResult.add(text);
			result.put("content", contentResult);
			result.put("structuredContent", structuredContent);
			response.put("result", result);
			return json(response, null);
		}
		response.put("error", error(-32601, "Method not found: " + method));
		return json(response, null);
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
			results.add(new MCPFragmentSearchResult(fragment.getArtifactId(), fragment.getPath(), fragment.getArtifactType(), fragment.getContentType(), fragment.getProperties(), fragment.isEditable(), fragment.isRemovable(), groupMatches(fragment.getMatches())));
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

	private Map<String, Object> error(int code, String message) {
		Map<String, Object> error = new LinkedHashMap<String, Object>();
		error.put("code", code);
		error.put("message", message);
		return error;
	}

	private Map<String, Object> schema(String type) {
		Map<String, Object> schema = new LinkedHashMap<String, Object>();
		schema.put("type", type);
		return schema;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> parse(InputStream content) throws IOException, ParseException {
		JSONBinding binding = new JSONBinding(new MapTypeGenerator(), Charset.forName("UTF-8"));
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
