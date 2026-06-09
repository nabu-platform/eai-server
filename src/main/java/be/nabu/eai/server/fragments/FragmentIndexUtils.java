package be.nabu.eai.server.fragments;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class FragmentIndexUtils {

	private FragmentIndexUtils() {
		// utility
	}

	public static List<String> filterValues(List<String> values) {
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

	public static boolean matchesNamespace(List<String> namespaces, String namespace) {
		if (namespaces == null || namespaces.isEmpty()) {
			return true;
		}
		for (String allowed : namespaces) {
			if (namespace.equals(allowed) || namespace.startsWith(allowed + ".")) {
				return true;
			}
		}
		return false;
	}

	public static boolean matchesGlob(List<String> globs, String namespace, String path) {
		if (globs == null || globs.isEmpty()) {
			return true;
		}
		String candidate = namespace + "/" + path;
		for (String glob : globs) {
			String regex = glob.replace("\\", "\\\\").replace(".", "\\.").replace("*", ".*").replace("?", ".");
			if (candidate.matches(regex) || path.matches(regex)) {
				return true;
			}
		}
		return false;
	}

	public static String toSqlLike(String glob) {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < glob.length(); i++) {
			char current = glob.charAt(i);
			if (current == '*') {
				builder.append('%');
			}
			else if (current == '?') {
				builder.append('_');
			}
			else if (current == '%' || current == '_' || current == '\\') {
				builder.append('\\').append(current);
			}
			else {
				builder.append(current);
			}
		}
		return builder.toString();
	}

	public static String placeholders(int amount) {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < amount; i++) {
			if (i > 0) {
				builder.append(", ");
			}
			builder.append("?");
		}
		return builder.toString();
	}

	public static String hash(String content) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashed = digest.digest((content == null ? "" : content).getBytes(StandardCharsets.UTF_8));
			StringBuilder builder = new StringBuilder();
			for (byte part : hashed) {
				builder.append(String.format("%02x", part));
			}
			return builder.toString();
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static Map<String, String> loadMap(Path path) throws IOException {
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

	public static void writeMap(Path path, Map<String, String> values) throws IOException {
		Map<String, String> snapshot = new LinkedHashMap<String, String>(values);
		Properties properties = new Properties();
		for (Map.Entry<String, String> entry : snapshot.entrySet()) {
			if (entry.getValue() != null) {
				properties.setProperty(entry.getKey(), entry.getValue());
			}
		}
		try (java.io.OutputStream output = Files.newOutputStream(path, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING)) {
			properties.store(output, null);
		}
	}

	public static String serializeProperties(Map<String, String> properties) {
		if (properties == null || properties.isEmpty()) {
			return null;
		}
		List<String> keys = new ArrayList<String>(properties.keySet());
		Collections.sort(keys);
		StringBuilder builder = new StringBuilder();
		for (String key : keys) {
			if (builder.length() > 0) {
				builder.append('\n');
			}
			builder.append(key).append('=').append(properties.get(key) == null ? "" : properties.get(key));
		}
		return builder.toString();
	}

	public static Map<String, String> deserializeProperties(String serialized) {
		if (serialized == null || serialized.isEmpty()) {
			return Collections.emptyMap();
		}
		LinkedHashMap<String, String> properties = new LinkedHashMap<String, String>();
		String[] lines = serialized.split("\\n", -1);
		for (String line : lines) {
			int index = line.indexOf('=');
			if (index < 0) {
				properties.put(line, "");
			}
			else {
				properties.put(line.substring(0, index), line.substring(index + 1));
			}
		}
		return properties;
	}

	public static String encodeId(String id) {
		return java.net.URLEncoder.encode(id, StandardCharsets.UTF_8).replace("+", "%20");
	}

	public static String decodeId(String id) {
		return java.net.URLDecoder.decode(id, StandardCharsets.UTF_8);
	}

	public static void deleteRecursively(Path path) throws IOException {
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
}
