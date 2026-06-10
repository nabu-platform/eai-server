package be.nabu.eai.server.fragments;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.eai.repository.api.ArtifactFragmentManager.ArtifactFragment;

public class FileSystemFragmentIndexBackend implements FragmentIndexBackend {

	public static final String MCP_PATH = "mcp.path";
	private static final String REGISTRY_FILE = "artifact-index.properties";
	private static final String REGISTRY_PREFIX = "fragment.";
	private static final String MARKER_MTIME_PREFIX = "mtime:";
	private static final String MARKER_HASH_PREFIX = "hash:";
	private Logger logger = LoggerFactory.getLogger(getClass());
	private Path root;
	private Map<String, String> registry;
	private Set<String> seenArtifacts;
	private Set<String> touchedFragments;
	private boolean registryAvailable;
	private boolean rebuilding;
	private final Object registryLock = new Object();

	public FileSystemFragmentIndexBackend(Path root) {
		this.root = root;
	}

	@Override
	public void initialize() {
		synchronized (registryLock) {
			try {
				Files.createDirectories(root);
				ensureRipgrep();
				Path registryFile = registryFile();
				registryAvailable = Files.exists(registryFile);
				registry = registryAvailable ? loadRegistry(registryFile) : new LinkedHashMap<String, String>();
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void beginRebuild() {
		synchronized (registryLock) {
			rebuilding = true;
			if (registry == null) {
				Path registryFile = registryFile();
				try {
					registryAvailable = Files.exists(registryFile);
					registry = registryAvailable ? loadRegistry(registryFile) : new LinkedHashMap<String, String>();
				}
				catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
			seenArtifacts = new HashSet<String>();
			touchedFragments = new HashSet<String>();
		}
	}

	@Override
	public void index(String artifactId, String artifactType, String artifactCategory, long version, List<ArtifactFragment> fragments) {
		synchronized (registryLock) {
			Path artifactRoot = artifactRoot(artifactId);
			try {
				Files.createDirectories(artifactRoot);
				deleteMissing(artifactId, artifactRoot, fragments);
				for (ArtifactFragment fragment : fragments) {
					writeFragment(artifactId, artifactRoot, artifactType, artifactCategory, fragment);
				}
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void finalizeRebuild() {
		synchronized (registryLock) {
			if (registry == null || seenArtifacts == null) {
				return;
			}
			try {
				Set<String> knownArtifacts = new HashSet<String>();
				for (String key : new ArrayList<String>(registry.keySet())) {
					String artifactId = artifactIdFromRegistryKey(key);
					if (artifactId == null) {
						continue;
					}
					knownArtifacts.add(artifactId);
					if (touchedFragments.contains(key)) {
						continue;
					}
					Path artifactRoot = artifactRoot(artifactId);
					String fragmentPath = fragmentPathFromRegistryKey(key, artifactId);
					Path file = artifactRoot.resolve(fragmentPath);
					Files.deleteIfExists(file);
					Files.deleteIfExists(propertiesFile(file));
					deleteEmptyParents(file.getParent(), artifactRoot);
					registry.remove(key);
				}
				for (String artifactId : knownArtifacts) {
					if (!seenArtifacts.contains(artifactId)) {
						deleteRecursively(artifactRoot(artifactId));
					}
				}
				writeRegistry(registryFile(), registry);
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
			finally {
				seenArtifacts = null;
				touchedFragments = null;
				rebuilding = false;
				registryAvailable = true;
			}
		}
	}

	@Override
	public void delete(String artifactId) {
		synchronized (registryLock) {
			Path artifactRoot = artifactRoot(artifactId);
			try {
				deleteRecursively(artifactRoot);
				removeArtifactEntries(registry, artifactId);
				if (!rebuilding) {
					writeRegistry(registryFile(), registry);
					registryAvailable = true;
				}
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public FragmentSearch get(String artifactId, String path) {
		List<FragmentSearch> fragments = get(Collections.singletonList(artifactId), Collections.singletonList(path));
		return fragments.isEmpty() ? null : fragments.get(0);
	}

	@Override
	public List<FragmentSearch> get(List<String> artifactIds, List<String> paths) {
		List<String> filteredArtifactIds = filterValues(artifactIds);
		List<String> filteredPaths = filterValues(paths);
		if (filteredArtifactIds.isEmpty() || filteredPaths.isEmpty()) {
			return Collections.emptyList();
		}
		try {
			List<FragmentSearch> results = new ArrayList<FragmentSearch>();
			for (String artifactId : filteredArtifactIds) {
				Path artifactRoot = artifactRoot(artifactId);
				for (String path : filteredPaths) {
					Path file = artifactRoot.resolve(path);
					if (!Files.isRegularFile(file)) {
						continue;
					}
					FragmentSearch fragment = toFragmentSearch(file, artifactRoot, null);
					if (fragment != null) {
						results.add(fragment);
					}
				}
			}
			return results;
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<FragmentSearch> list(List<String> globs, List<String> namespaces, List<String> artifactTypes, List<String> artifactCategories) {
		try {
			List<Path> files = new ArrayList<Path>();
			java.nio.file.Files.walk(root)
				.filter(Files::isRegularFile)
				.filter(path -> !path.getFileName().toString().endsWith(".properties"))
				.forEach(files::add);
			return listFiles(files, filterValues(globs), filterValues(namespaces), filterValues(artifactTypes), filterValues(artifactCategories));
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<FragmentSearch> search(String pattern, List<String> globs, List<String> namespaces, List<String> artifactTypes, List<String> artifactCategories, boolean caseSensitive, int before, int after, int limit) {
		ensureRipgrep();
		List<String> command = new ArrayList<String>();
		command.add("rg");
		command.add("--line-number");
		command.add("--with-filename");
		command.add("--color");
		command.add("never");
		command.add("--no-heading");
		if (!caseSensitive) {
			command.add("--ignore-case");
		}
		command.add("--glob");
		command.add("!*.properties");
		for (String glob : filterValues(globs)) {
			command.add("--glob");
			command.add(glob);
		}
		if (before > 0) {
			command.add("--before-context");
			command.add(Integer.toString(before));
		}
		if (after > 0) {
			command.add("--after-context");
			command.add(Integer.toString(after));
		}
		if (limit > 0) {
			command.add("--max-count");
			command.add(Integer.toString(limit));
		}
		command.add(pattern);
		List<String> filteredNamespaces = filterValues(namespaces);
		command.add(root.toAbsolutePath().toString());
		Process process = null;
		try {
			process = new ProcessBuilder(command).redirectErrorStream(true).start();
			List<String> lines = Files.readAllLines(writeTemp(process), StandardCharsets.UTF_8);
			int exitCode = process.waitFor();
			if (exitCode != 0 && exitCode != 1) {
				throw new RuntimeException("rg failed with exit code " + exitCode + ": " + lines);
			}
			return parse(lines, filteredNamespaces, filterValues(artifactTypes), filterValues(artifactCategories));
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private Path writeTemp(Process process) throws IOException {
		Path temp = Files.createTempFile("fragment-index-rg", ".txt");
		Files.write(temp, read(process), StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING);
		return temp;
	}

	private List<String> read(Process process) throws IOException {
		return java.nio.file.Files.readAllLines(copy(process), StandardCharsets.UTF_8);
	}

	private Path copy(Process process) throws IOException {
		Path temp = Files.createTempFile("fragment-index-rg-stream", ".txt");
		Files.copy(process.getInputStream(), temp, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
		return temp;
	}

	private List<FragmentSearch> parse(List<String> lines, List<String> namespaces, List<String> artifactTypes, List<String> artifactCategories) throws IOException {
		Map<String, List<String>> grouped = new LinkedHashMap<String, List<String>>();
		String currentFile = null;
		for (String line : lines) {
			if (line == null || line.trim().isEmpty()) {
				continue;
			}
			if ("--".equals(line.trim())) {
				if (currentFile != null) {
					grouped.computeIfAbsent(currentFile, key -> new ArrayList<String>()).add("--");
				}
				continue;
			}
			ParsedSearchLine parsed = parseSearchLine(line);
			if (parsed == null) {
				continue;
			}
			currentFile = parsed.file;
			grouped.computeIfAbsent(parsed.file, key -> new ArrayList<String>()).add(parsed.content);
		}
		List<FragmentSearch> results = new ArrayList<FragmentSearch>();
		for (Map.Entry<String, List<String>> entry : grouped.entrySet()) {
			Path file = Paths.get(entry.getKey());
			if (file.getFileName().toString().endsWith(".properties")) {
				continue;
			}
			Path artifactRoot = resolveArtifactRoot(file);
			Map<String, String> fragment = loadProperties(propertiesFile(file));
			Map<String, String> properties = new LinkedHashMap<String, String>(fragment);
			properties.remove("hash");
			properties.remove("artifactType");
			properties.remove("artifactCategory");
			properties.remove("fragmentType");
			properties.remove("contentType");
			properties.remove("editable");
			properties.remove("removable");
			String artifactId = decodeArtifactId(artifactRoot.getFileName().toString());
			if (!matchesNamespace(namespaces, artifactId)) {
				continue;
			}
			if (!artifactTypes.isEmpty() && !artifactTypes.contains(fragment.get("artifactType"))) {
				continue;
			}
			if (!artifactCategories.isEmpty() && !artifactCategories.contains(fragment.get("artifactCategory"))) {
				continue;
			}
			results.add(new FragmentSearch(artifactId, relativizeFragment(artifactRoot, file), fragment.get("artifactType"), fragment.get("artifactCategory"), fragment.get("fragmentType"), Files.readString(file, StandardCharsets.UTF_8), fragment.get("contentType"), properties, entry.getValue(), Boolean.parseBoolean(fragment.get("editable")), Boolean.parseBoolean(fragment.get("removable"))));
		}
		return results;
	}

	private List<FragmentSearch> listFiles(List<Path> files, List<String> globs, List<String> namespaces, List<String> artifactTypes, List<String> artifactCategories) throws IOException {
		List<FragmentSearch> results = new ArrayList<FragmentSearch>();
		for (Path file : files) {
			Path artifactRoot = resolveArtifactRoot(file);
			FragmentSearch fragment = toFragmentSearch(file, artifactRoot, null);
			if (fragment == null) {
				continue;
			}
			if (!matchesNamespace(namespaces, fragment.getArtifactId())) {
				continue;
			}
			if (!matchesGlob(globs, fragment.getArtifactId(), fragment.getPath())) {
				continue;
			}
			if (!artifactTypes.isEmpty() && !artifactTypes.contains(fragment.getArtifactType())) {
				continue;
			}
			if (!artifactCategories.isEmpty() && !artifactCategories.contains(fragment.getArtifactCategory())) {
				continue;
			}
			results.add(fragment);
		}
		return results;
	}

	private FragmentSearch toFragmentSearch(Path file, Path artifactRoot, List<String> matches) throws IOException {
		Map<String, String> fragment = loadProperties(propertiesFile(file));
		Map<String, String> properties = new LinkedHashMap<String, String>(fragment);
		properties.remove("hash");
		properties.remove("artifactType");
		properties.remove("artifactCategory");
		properties.remove("fragmentType");
		properties.remove("contentType");
		properties.remove("editable");
		properties.remove("removable");
		String artifactId = decodeArtifactId(artifactRoot.getFileName().toString());
		String path = relativizeFragment(artifactRoot, file);
		return new FragmentSearch(artifactId, path, fragment.get("artifactType"), fragment.get("artifactCategory"), fragment.get("fragmentType"), Files.readString(file, StandardCharsets.UTF_8), fragment.get("contentType"), properties, matches == null ? Collections.<String>emptyList() : matches, Boolean.parseBoolean(fragment.get("editable")), Boolean.parseBoolean(fragment.get("removable")));
	}

	private boolean matchesGlob(List<String> globs, String artifactId, String path) {
		if (globs == null || globs.isEmpty()) {
			return true;
		}
		String candidate = artifactId + "/" + path;
		for (String glob : globs) {
			String regex = glob.replace("\\", "\\\\").replace(".", "\\.").replace("*", ".*").replace("?", ".");
			if (candidate.matches(regex)) {
				return true;
			}
		}
		return false;
	}

	private List<String> filterValues(List<String> values) {
		if (values == null || values.isEmpty()) {
			return Collections.emptyList();
		}
		List<String> filtered = new ArrayList<String>();
		for (String value : values) {
			if (value != null) {
				value = value.trim();
				if (!value.isEmpty()) {
					filtered.add(value);
				}
			}
		}
		return filtered;
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

	private ParsedSearchLine parseSearchLine(String line) {
		for (int i = 0; i < line.length(); i++) {
			char separator = line.charAt(i);
			if (separator != ':' && separator != '-' && separator != '+') {
				continue;
			}
			int numberStart = i + 1;
			if (numberStart >= line.length() || !Character.isDigit(line.charAt(numberStart))) {
				continue;
			}
			int numberEnd = numberStart + 1;
			while (numberEnd < line.length() && Character.isDigit(line.charAt(numberEnd))) {
				numberEnd++;
			}
			if (numberEnd >= line.length() || line.charAt(numberEnd) != separator) {
				continue;
			}
			return new ParsedSearchLine(line.substring(0, i), line.substring(i + 1));
		}
		return null;
	}

	private String relativizeFragment(Path artifactRoot, Path file) {
		return artifactRoot.relativize(file).toString().replace('\\', '/');
	}

	private void writeFragment(String artifactId, Path artifactRoot, String artifactType, String artifactCategory, ArtifactFragment fragment) throws IOException {
		Path file = artifactRoot.resolve(fragment.getPath());
		Path propertiesFile = propertiesFile(file);
		Files.createDirectories(file.getParent());
		String registryKey = registryKey(artifactId, fragment.getPath());
		if (touchedFragments != null) {
			touchedFragments.add(registryKey);
		}
		String registryMarker = registry.get(registryKey);
		String marker = marker(fragment);
		if (marker != null && Files.exists(file) && marker.equals(registryMarker)) {
			registry.put(registryKey, marker);
			return;
		}
		String contentValue = fragment.getContent();
		String content = contentValue == null ? "" : contentValue;
		String hash = hash(content);
		String fallbackMarker = marker(fragment, hash);
		if (Files.exists(file) && fallbackMarker.equals(registryMarker)) {
			registry.put(registryKey, fallbackMarker);
			return;
		}
		Map<String, String> values = fragmentProperties(artifactType, artifactCategory, fragment, hash);
		Files.write(file, content.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
		writeProperties(propertiesFile, values);
		registry.put(registryKey, fallbackMarker);
		if (!rebuilding) {
			writeRegistry(registryFile(), registry);
			registryAvailable = true;
		}
	}

	private Map<String, String> fragmentProperties(String artifactType, String artifactCategory, ArtifactFragment fragment, String hash) {
		Map<String, String> values = new LinkedHashMap<String, String>();
		values.put("hash", hash);
		values.put("artifactType", artifactType);
		values.put("artifactCategory", artifactCategory);
		values.put("fragmentType", fragment.getFragmentType());
		values.put("contentType", fragment.getContentType());
		values.put("editable", Boolean.toString(fragment.isEditable()));
		values.put("removable", Boolean.toString(fragment.isRemovable()));
		if (fragment.getProperties() != null && !fragment.getProperties().isEmpty()) {
			values.putAll(fragment.getProperties());
		}
		return values;
	}

	private Map<String, String> loadProperties(Path path) throws IOException {
		return loadMap(path);
	}

	private void writeProperties(Path path, Map<String, String> values) throws IOException {
		writeMap(path, values);
	}

	private Map<String, String> loadRegistry(Path path) throws IOException {
		return loadMap(path);
	}

	private void writeRegistry(Path path, Map<String, String> values) throws IOException {
		writeMap(path, values);
	}

	private Map<String, String> loadMap(Path path) throws IOException {
		if (!Files.exists(path)) {
			return Collections.emptyMap();
		}
		Properties properties = new Properties();
		try (java.io.InputStream input = Files.newInputStream(path)) {
			properties.load(input);
		}
		Map<String, String> result = new LinkedHashMap<String, String>();
		for (String name : properties.stringPropertyNames()) {
			result.put(name, properties.getProperty(name));
		}
		return result;
	}

	private void writeMap(Path path, Map<String, String> values) throws IOException {
		Map<String, String> snapshot = new LinkedHashMap<String, String>(values);
		Properties properties = new Properties();
		for (Map.Entry<String, String> entry : snapshot.entrySet()) {
			if (entry.getValue() != null) {
				properties.setProperty(entry.getKey(), entry.getValue());
			}
		}
		try (java.io.OutputStream output = Files.newOutputStream(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			properties.store(output, null);
		}
	}

	private static class ParsedSearchLine {
		private final String file;
		private final String content;

		private ParsedSearchLine(String file, String content) {
			this.file = file;
			this.content = content;
		}
	}

	private boolean isEmpty(Path path) throws IOException {
		try (java.util.stream.Stream<Path> stream = Files.list(path)) {
			return !stream.findFirst().isPresent();
		}
	}

	private void deleteMissing(String artifactId, Path artifactRoot, List<ArtifactFragment> fragments) throws IOException {
		if (seenArtifacts != null) {
			seenArtifacts.add(artifactId);
		}
		if (rebuilding && !registryAvailable) {
			deleteMissingLegacy(artifactRoot, fragments);
			return;
		}
		if (!rebuilding) {
			Set<String> current = new HashSet<String>();
			for (ArtifactFragment fragment : fragments) {
				current.add(registryKey(artifactId, fragment.getPath()));
			}
			for (String key : new ArrayList<String>(registry.keySet())) {
				if (!isArtifactRegistryKey(key, artifactId) || current.contains(key)) {
					continue;
				}
				String fragmentPath = fragmentPathFromRegistryKey(key, artifactId);
				Path file = artifactRoot.resolve(fragmentPath);
				Files.deleteIfExists(file);
				Files.deleteIfExists(propertiesFile(file));
				deleteEmptyParents(file.getParent(), artifactRoot);
				registry.remove(key);
			}
			writeRegistry(registryFile(), registry);
			registryAvailable = true;
		}
	}

	private void deleteMissingLegacy(Path artifactRoot, List<ArtifactFragment> fragments) throws IOException {
		List<Path> keep = new ArrayList<Path>();
		for (ArtifactFragment fragment : fragments) {
			Path fragmentFile = artifactRoot.resolve(fragment.getPath());
			keep.add(fragmentFile);
			keep.add(propertiesFile(fragmentFile));
		}
		if (!Files.exists(artifactRoot)) {
			return;
		}
		Files.walk(artifactRoot)
			.sorted(java.util.Comparator.reverseOrder())
			.forEach(path -> {
				try {
					if (!Files.isDirectory(path) && !keep.contains(path)) {
						Files.deleteIfExists(path);
					}
					else if (Files.isDirectory(path) && !path.equals(artifactRoot) && isEmpty(path)) {
						Files.deleteIfExists(path);
					}
				}
				catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
	}

	private void deleteEmptyParents(Path path, Path artifactRoot) throws IOException {
		while (path != null && !path.equals(artifactRoot)) {
			if (!Files.exists(path) || !isEmpty(path)) {
				return;
			}
			Files.deleteIfExists(path);
			path = path.getParent();
		}
	}

	private void deleteRecursively(Path path) throws IOException {
		if (!Files.exists(path)) {
			return;
		}
		Files.walk(path)
			.sorted(java.util.Comparator.reverseOrder())
			.forEach(single -> {
				try {
					Files.deleteIfExists(single);
				}
				catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
	}

	private Path artifactRoot(String artifactId) {
		return root.resolve(encodeArtifactId(artifactId));
	}

	private Path registryFile() {
		return root.resolve(REGISTRY_FILE);
	}

	private String registryKey(String artifactId, String fragmentPath) {
		return REGISTRY_PREFIX + encodeArtifactId(artifactId) + "/" + fragmentPath;
	}

	private boolean isArtifactRegistryKey(String key, String artifactId) {
		return key.startsWith(REGISTRY_PREFIX + encodeArtifactId(artifactId) + "/");
	}

	private String fragmentPathFromRegistryKey(String key, String artifactId) {
		return key.substring((REGISTRY_PREFIX + encodeArtifactId(artifactId) + "/").length());
	}

	private String artifactIdFromRegistryKey(String key) {
		if (!key.startsWith(REGISTRY_PREFIX)) {
			return null;
		}
		String remaining = key.substring(REGISTRY_PREFIX.length());
		int separator = remaining.indexOf('/');
		if (separator < 0) {
			return null;
		}
		return decodeArtifactId(remaining.substring(0, separator));
	}

	private Path propertiesFile(Path file) {
		return file.resolveSibling(file.getFileName().toString() + ".properties");
	}

	private Path resolveArtifactRoot(Path file) {
		Path current = file.getParent();
		while (current != null && !root.equals(current.getParent())) {
			current = current.getParent();
		}
		return current;
	}

	private String encodeArtifactId(String artifactId) {
		return java.net.URLEncoder.encode(artifactId, StandardCharsets.UTF_8).replace("+", "%20");
	}

	private String decodeArtifactId(String artifactId) {
		return java.net.URLDecoder.decode(artifactId, StandardCharsets.UTF_8);
	}

	private void removeArtifactEntries(String artifactId) {
		removeArtifactEntries(registry, artifactId);
	}

	private void removeArtifactEntries(Map<String, String> values, String artifactId) {
		if (values == null) {
			return;
		}
		for (String key : new ArrayList<String>(values.keySet())) {
			if (isArtifactRegistryKey(key, artifactId)) {
				values.remove(key);
			}
		}
	}

	private void ensureRipgrep() {
		try {
			Process process = new ProcessBuilder("rg", "--version").redirectErrorStream(true).start();
			int exitCode = process.waitFor();
			if (exitCode != 0) {
				throw new IllegalStateException("rg is not available");
			}
		}
		catch (Exception e) {
			logger.error("ripgrep is required for filesystem fragment indexing", e);
			throw new RuntimeException("ripgrep is required for filesystem fragment indexing", e);
		}
	}

	private String marker(ArtifactFragment fragment) {
		Long lastModified = fragment.getLastModified();
		return lastModified == null ? null : MARKER_MTIME_PREFIX + lastModified;
	}

	private String marker(ArtifactFragment fragment, String hash) {
		Long lastModified = fragment.getLastModified();
		return lastModified == null ? MARKER_HASH_PREFIX + hash : MARKER_MTIME_PREFIX + lastModified;
	}

	private String hash(String content) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashed = digest.digest(content.getBytes(StandardCharsets.UTF_8));
			StringBuilder builder = new StringBuilder();
			for (byte part : hashed) {
				builder.append(String.format("%02x", part));
			}
			return builder.toString();
		}
		catch (Exception e) {
			logger.error("Could not hash fragment content", e);
			throw new RuntimeException(e);
		}
	}

}
