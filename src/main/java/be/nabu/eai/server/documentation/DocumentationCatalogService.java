package be.nabu.eai.server.documentation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.eai.repository.EAIResourceRepository;
import be.nabu.eai.repository.api.Entry;
import be.nabu.eai.repository.api.Repository;
import be.nabu.eai.repository.api.ResourceEntry;
import be.nabu.eai.server.fragments.FragmentIndexUtils;
import be.nabu.eai.server.fragments.RipgrepFormatter;
import be.nabu.libs.resources.ResourceUtils;
import be.nabu.libs.resources.api.ManageableContainer;
import be.nabu.libs.resources.api.ReadableResource;
import be.nabu.libs.resources.api.Resource;
import be.nabu.libs.resources.api.ResourceContainer;
import be.nabu.libs.resources.api.TimestampedResource;
import be.nabu.libs.resources.api.WritableResource;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.ReadableContainer;
import be.nabu.utils.io.api.WritableContainer;

public class DocumentationCatalogService {

	private static final String DOCUMENTATION_PATH = EAIResourceRepository.PROTECTED + "/documentation";
	private Logger logger = LoggerFactory.getLogger(getClass());
	private Repository repository;
	private ConcurrentMap<String, DocumentationSearch> catalog = new ConcurrentHashMap<String, DocumentationSearch>();

	public DocumentationCatalogService(Repository repository) {
		this.repository = repository;
	}

	public void rebuild() {
		Map<String, DocumentationSearch> snapshot = new LinkedHashMap<String, DocumentationSearch>();
		refresh(repository.getRoot(), snapshot);
		catalog.clear();
		catalog.putAll(snapshot);
	}

	public void refresh(String namespace) {
		Entry entry = repository.getEntry(namespace);
		for (String key : new ArrayList<String>(catalog.keySet())) {
			if (key.startsWith(namespace + ":")) {
				catalog.remove(key);
			}
		}
		if (entry != null) {
			Map<String, DocumentationSearch> snapshot = new LinkedHashMap<String, DocumentationSearch>();
			refresh(entry, snapshot);
			catalog.putAll(snapshot);
		}
	}

	public List<DocumentationSearch> list(List<String> globs, List<String> namespaces) {
		List<String> filteredGlobs = FragmentIndexUtils.filterValues(globs);
		List<String> filteredNamespaces = FragmentIndexUtils.filterValues(namespaces);
		List<DocumentationSearch> results = new ArrayList<DocumentationSearch>();
		for (DocumentationSearch document : catalog.values()) {
			if (!FragmentIndexUtils.matchesNamespace(filteredNamespaces, document.getNamespace())) {
				continue;
			}
			if (!FragmentIndexUtils.matchesGlob(filteredGlobs, document.getNamespace(), document.getPath())) {
				continue;
			}
			results.add(document);
		}
		Collections.sort(results, (left, right) -> (left.getNamespace() + "/" + left.getPath()).compareTo(right.getNamespace() + "/" + right.getPath()));
		return results;
	}

	public List<DocumentationSearch> search(String pattern, List<String> globs, List<String> namespaces, boolean caseSensitive, int before, int after, int limit) {
		Pattern compiled;
		try {
			compiled = Pattern.compile(pattern, caseSensitive ? 0 : Pattern.CASE_INSENSITIVE);
		}
		catch (PatternSyntaxException e) {
			throw new IllegalArgumentException("Invalid regex pattern: " + pattern, e);
		}
		List<DocumentationSearch> results = new ArrayList<DocumentationSearch>();
		for (DocumentationSearch document : list(globs, namespaces)) {
			List<String> matches = RipgrepFormatter.format(document.getContent(), compiled, before, after);
			if (!matches.isEmpty()) {
				results.add(new DocumentationSearch(document.getNamespace(), document.getPath(), document.getContent(), document.getContentType(), document.getProperties(), matches, document.isEditable(), document.isRemovable(), document.getLastModified()));
				if (limit > 0 && results.size() >= limit) {
					break;
				}
			}
		}
		return results;
	}

	public DocumentationSearch get(String namespace, String path) {
		return catalog.get(key(namespace, path));
	}

	public DocumentationSearch write(String namespace, String path, String content, String mode) throws IOException {
		Resource resource = documentationResource(namespace, path, true);
		if (!(resource instanceof WritableResource)) {
			throw new IOException("Documentation is not writable: " + namespace + "/" + path);
		}
		String before = resource instanceof ReadableResource ? read((ReadableResource) resource) : "";
		String after;
		if ("append".equals(mode)) {
			after = before + content;
		}
		else if ("prepend".equals(mode)) {
			after = content + before;
		}
		else {
			after = content;
		}
		write((WritableResource) resource, after);
		Resource root = documentationRoot(namespace, false);
		if (!(root instanceof ResourceContainer)) {
			throw new IOException("Documentation namespace not found: " + namespace);
		}
		DocumentationSearch document = toDocument(namespace, (ResourceContainer<?>) root, resource);
		catalog.put(key(namespace, path), document);
		return document;
	}

	public void delete(String namespace, String path) throws IOException {
		Resource root = documentationRoot(namespace, false);
		if (!(root instanceof ResourceContainer)) {
			throw new IOException("Documentation namespace not found: " + namespace);
		}
		Resource resource = ResourceUtils.resolve(root, path);
		if (resource == null) {
			throw new IOException("Documentation not found: " + namespace + "/" + path);
		}
		Resource parent = resource.getParent();
		if (!(parent instanceof ManageableContainer)) {
			throw new IOException("Documentation is not removable: " + namespace + "/" + path);
		}
		((ManageableContainer<?>) parent).delete(resource.getName());
		catalog.remove(key(namespace, path));
	}

	private void refresh(Entry entry, Map<String, DocumentationSearch> target) {
		if (entry == null) {
			return;
		}
		if (entry instanceof ResourceEntry) {
			try {
				Resource root = documentationRoot((ResourceEntry) entry, false);
				if (root instanceof ResourceContainer) {
					collect(entry.getId(), (ResourceContainer<?>) root, (ResourceContainer<?>) root, target);
				}
			}
			catch (Exception e) {
				logger.error("Could not refresh documentation catalog for namespace: " + entry.getId(), e);
			}
		}
		for (Entry child : entry) {
			refresh(child, target);
		}
	}

	private void collect(String namespace, ResourceContainer<?> root, Resource resource, Map<String, DocumentationSearch> target) throws IOException {
		if (resource instanceof ResourceContainer) {
			for (Resource child : (ResourceContainer<?>) resource) {
				collect(namespace, root, child, target);
			}
			return;
		}
		if (!(resource instanceof ReadableResource) || !isText(resource)) {
			return;
		}
		DocumentationSearch document = toDocument(namespace, root, resource);
		target.put(key(namespace, document.getPath()), document);
	}

	private DocumentationSearch toDocument(String namespace, ResourceContainer<?> root, Resource resource) throws IOException {
		String path = relativePath(root, resource);
		return new DocumentationSearch(namespace, path, read((ReadableResource) resource), resource.getContentType(), Collections.<String, String>emptyMap(), Collections.<String>emptyList(), resource instanceof WritableResource, resource.getParent() instanceof ManageableContainer, lastModified(resource));
	}

	private Resource documentationResource(String namespace, String path, boolean create) throws IOException {
		Resource root = documentationRoot(namespace, create);
		if (!(root instanceof ResourceContainer)) {
			throw new IOException("Documentation namespace not found: " + namespace);
		}
		if (create) {
			return ResourceUtils.touch(root, path);
		}
		return ResourceUtils.resolve(root, path);
	}

	private Resource documentationRoot(String namespace, boolean create) throws IOException {
		Entry entry = repository.getEntry(namespace);
		if (!(entry instanceof ResourceEntry)) {
			throw new IOException("Namespace is not resource-backed: " + namespace);
		}
		return documentationRoot((ResourceEntry) entry, create);
	}

	private Resource documentationRoot(ResourceEntry entry, boolean create) throws IOException {
		if (create) {
			return ResourceUtils.mkdirs(entry.getContainer(), DOCUMENTATION_PATH);
		}
		return ResourceUtils.resolve(entry.getContainer(), DOCUMENTATION_PATH);
	}

	private String relativePath(ResourceContainer<?> root, Resource resource) {
		List<String> parts = new ArrayList<String>();
		Resource current = resource;
		while (current != null && current != root) {
			parts.add(0, current.getName());
			current = current.getParent();
		}
		StringBuilder builder = new StringBuilder();
		for (String part : parts) {
			if (builder.length() > 0) {
				builder.append('/');
			}
			builder.append(part);
		}
		return builder.toString();
	}

	private String read(ReadableResource resource) throws IOException {
		ReadableContainer<ByteBuffer> readable = resource.getReadable();
		try {
			return new String(IOUtils.toBytes(readable), StandardCharsets.UTF_8);
		}
		finally {
			readable.close();
		}
	}

	private void write(WritableResource resource, String content) throws IOException {
		WritableContainer<ByteBuffer> writable = resource.getWritable();
		try {
			writable.write(IOUtils.wrap((content == null ? "" : content).getBytes(StandardCharsets.UTF_8), true));
		}
		finally {
			writable.close();
		}
	}

	private Long lastModified(Resource resource) {
		return resource instanceof TimestampedResource && ((TimestampedResource) resource).getLastModified() != null ? ((TimestampedResource) resource).getLastModified().getTime() : null;
	}

	private boolean isText(Resource resource) {
		String contentType = resource.getContentType();
		String name = resource.getName();
		if (contentType != null && (contentType.startsWith("text/") || contentType.contains("json") || contentType.contains("xml") || contentType.contains("yaml"))) {
			return true;
		}
		return name.endsWith(".md") || name.endsWith(".html") || name.endsWith(".htm") || name.endsWith(".txt") || name.endsWith(".xml") || name.endsWith(".json") || name.endsWith(".yaml") || name.endsWith(".yml");
	}

	private String key(String namespace, String path) {
		return namespace + ":" + path;
	}
}
