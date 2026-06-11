package be.nabu.eai.server.fragments;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.eai.repository.api.ArtifactFragmentManager.ArtifactFragment;

public class JdbcFragmentIndexBackend implements FragmentIndexBackend {

	private Logger logger = LoggerFactory.getLogger(getClass());
	private static final int BATCH_SIZE = 500;
	private DataSource dataSource;
	private Connection rebuildConnection;
	private PreparedStatement rebuildUpsert;

	public JdbcFragmentIndexBackend(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	@Override
	public void initialize() {
		Connection connection = null;
		Statement statement = null;
		try {
			connection = dataSource.getConnection();
			statement = connection.createStatement();
			statement.execute("create table if not exists fragment_index (artifact_id varchar(255) not null, version bigint not null, path varchar(255) not null, artifact_type varchar(255), artifact_category varchar(255), fragment_type varchar(255), content_type varchar(255), content clob, properties clob, content_hash varchar(64), editable boolean not null, removable boolean not null, primary key (artifact_id, path))");
			statement.execute("alter table fragment_index add column if not exists artifact_category varchar(255)");
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
		finally {
			close(statement);
			close(connection);
		}
	}

	@Override
	public void beginRebuild() {
		try {
			rebuildConnection = dataSource.getConnection();
			rebuildConnection.setAutoCommit(false);
			rebuildUpsert = rebuildConnection.prepareStatement("MERGE INTO fragment_index AS target USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source (artifact_id, version, path, artifact_type, artifact_category, fragment_type, content_type, content, properties, content_hash, editable, removable) ON target.artifact_id = source.artifact_id AND target.path = source.path WHEN MATCHED THEN UPDATE SET version = source.version, artifact_type = source.artifact_type, artifact_category = source.artifact_category, fragment_type = source.fragment_type, content_type = source.content_type, content = source.content, properties = source.properties, content_hash = source.content_hash, editable = source.editable, removable = source.removable WHEN NOT MATCHED THEN INSERT (artifact_id, version, path, artifact_type, artifact_category, fragment_type, content_type, content, properties, content_hash, editable, removable) VALUES (source.artifact_id, source.version, source.path, source.artifact_type, source.artifact_category, source.fragment_type, source.content_type, source.content, source.properties, source.content_hash, source.editable, source.removable)");
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void index(String artifactId, String artifactType, String artifactCategory, long version, List<ArtifactFragment> fragments) {
		if (rebuildConnection != null) {
			try {
				indexBatch(rebuildConnection, rebuildUpsert, artifactId, artifactType, artifactCategory, version, fragments, true);
			}
			catch (SQLException e) {
				rollback(rebuildConnection);
				throw new RuntimeException(e);
			}
			return;
		}
		Connection connection = null;
		PreparedStatement delete = null;
		PreparedStatement upsert = null;
		try {
			connection = dataSource.getConnection();
			connection.setAutoCommit(false);
			deleteMissing(connection, artifactId, fragments);
			upsert = connection.prepareStatement("MERGE INTO fragment_index AS target USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source (artifact_id, version, path, artifact_type, artifact_category, fragment_type, content_type, content, properties, content_hash, editable, removable) ON target.artifact_id = source.artifact_id AND target.path = source.path WHEN MATCHED THEN UPDATE SET version = source.version, artifact_type = source.artifact_type, artifact_category = source.artifact_category, fragment_type = source.fragment_type, content_type = source.content_type, content = source.content, properties = source.properties, content_hash = source.content_hash, editable = source.editable, removable = source.removable WHEN NOT MATCHED THEN INSERT (artifact_id, version, path, artifact_type, artifact_category, fragment_type, content_type, content, properties, content_hash, editable, removable) VALUES (source.artifact_id, source.version, source.path, source.artifact_type, source.artifact_category, source.fragment_type, source.content_type, source.content, source.properties, source.content_hash, source.editable, source.removable)");
			indexBatch(connection, upsert, artifactId, artifactType, artifactCategory, version, fragments, false);
			connection.commit();
		}
		catch (SQLException e) {
			rollback(connection);
			throw new RuntimeException(e);
		}
		finally {
			close(upsert);
			close(delete);
			resetAutoCommit(connection);
			close(connection);
		}
	}

	@Override
	public void finalizeRebuild() {
		if (rebuildConnection == null) {
			return;
		}
		try {
			rebuildUpsert.executeBatch();
			rebuildConnection.commit();
		}
		catch (SQLException e) {
			rollback(rebuildConnection);
			throw new RuntimeException(e);
		}
		finally {
			close(rebuildUpsert);
			resetAutoCommit(rebuildConnection);
			close(rebuildConnection);
			rebuildUpsert = null;
			rebuildConnection = null;
		}
	}

	@Override
	public void delete(String artifactId) {
		Connection connection = null;
		PreparedStatement statement = null;
		try {
			connection = dataSource.getConnection();
			statement = connection.prepareStatement("delete from fragment_index where artifact_id = ?");
			statement.setString(1, artifactId);
			statement.executeUpdate();
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
		finally {
			close(statement);
			close(connection);
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
		Connection connection = null;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			connection = dataSource.getConnection();
			StringBuilder sql = new StringBuilder("select artifact_id, path, artifact_type, artifact_category, fragment_type, content, content_type, properties, editable, removable from fragment_index where artifact_id in (" + placeholders(filteredArtifactIds.size()) + ") and path in (" + placeholders(filteredPaths.size()) + ") order by artifact_id, path");
			statement = connection.prepareStatement(sql.toString());
			int parameter = 1;
			for (String artifactId : filteredArtifactIds) {
				statement.setString(parameter++, artifactId);
			}
			for (String path : filteredPaths) {
				statement.setString(parameter++, path);
			}
			resultSet = statement.executeQuery();
			List<FragmentSearch> results = new ArrayList<FragmentSearch>();
			while (resultSet.next()) {
				results.add(mapFragment(resultSet, Collections.<String>emptyList()));
			}
			return results;
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
		finally {
			close(resultSet);
			close(statement);
			close(connection);
		}
	}

	@Override
	public List<FragmentSearch> list(List<String> globs, List<String> namespaces, List<String> artifactTypes, List<String> artifactCategories) {
		Connection connection = null;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		List<String> filteredGlobs = filterValues(globs);
		List<String> filteredNamespaces = filterValues(namespaces);
		List<String> filteredArtifactTypes = filterValues(artifactTypes);
		List<String> filteredArtifactCategories = filterValues(artifactCategories);
		if (namespaces != null && filteredNamespaces.isEmpty()) {
			return Collections.emptyList();
		}
		try {
			connection = dataSource.getConnection();
			StringBuilder sql = new StringBuilder("select artifact_id, path, artifact_type, artifact_category, fragment_type, content, content_type, properties, editable, removable from fragment_index");
			boolean hasWhere = false;
			if (!filteredNamespaces.isEmpty()) {
				sql.append(" where (");
				for (int i = 0; i < filteredNamespaces.size(); i++) {
					if (i > 0) {
						sql.append(" or ");
					}
					sql.append("artifact_id = ? or artifact_id like ?");
				}
				sql.append(")");
				hasWhere = true;
			}
			if (!filteredGlobs.isEmpty()) {
				sql.append(hasWhere ? " and (" : " where (");
				for (int i = 0; i < filteredGlobs.size(); i++) {
					if (i > 0) {
						sql.append(" or ");
					}
					sql.append("artifact_id || '/' || path like ? escape '\\\\'");
				}
				sql.append(")");
				hasWhere = true;
			}
			if (!filteredArtifactTypes.isEmpty()) {
				sql.append(hasWhere ? " and (" : " where (");
				for (int i = 0; i < filteredArtifactTypes.size(); i++) {
					if (i > 0) {
						sql.append(" or ");
					}
					sql.append("artifact_type = ?");
				}
				sql.append(")");
				hasWhere = true;
			}
			if (!filteredArtifactCategories.isEmpty()) {
				sql.append(hasWhere ? " and (" : " where (");
				for (int i = 0; i < filteredArtifactCategories.size(); i++) {
					if (i > 0) {
						sql.append(" or ");
					}
					sql.append("artifact_category = ?");
				}
				sql.append(")");
			}
			sql.append(" order by artifact_id, path");
			statement = connection.prepareStatement(sql.toString());
			int parameter = 1;
			for (String namespace : filteredNamespaces) {
				statement.setString(parameter++, namespace);
				statement.setString(parameter++, namespace + ".%");
			}
			for (String glob : filteredGlobs) {
				statement.setString(parameter++, toSqlLike(glob));
			}
			for (String artifactType : filteredArtifactTypes) {
				statement.setString(parameter++, artifactType);
			}
			for (String artifactCategory : filteredArtifactCategories) {
				statement.setString(parameter++, artifactCategory);
			}
			resultSet = statement.executeQuery();
			List<FragmentSearch> results = new ArrayList<FragmentSearch>();
			while (resultSet.next()) {
				results.add(mapFragment(resultSet, Collections.<String>emptyList()));
			}
			return results;
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
		finally {
			close(resultSet);
			close(statement);
			close(connection);
		}
	}

	@Override
	public List<FragmentSearch> search(String pattern, List<String> globs, List<String> namespaces, List<String> artifactTypes, List<String> artifactCategories, boolean caseSensitive, int before, int after, int limit) {
		Pattern compiled;
		try {
			compiled = Pattern.compile(pattern, caseSensitive ? 0 : Pattern.CASE_INSENSITIVE);
		}
		catch (PatternSyntaxException e) {
			throw new IllegalArgumentException("Invalid regex pattern: " + pattern, e);
		}
		Connection connection = null;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			connection = dataSource.getConnection();
			StringBuilder sql = new StringBuilder("select artifact_id, path, version, artifact_type, artifact_category, fragment_type, content_type, content, properties, editable, removable from fragment_index");
			List<String> filteredGlobs = filterValues(globs);
			List<String> filteredNamespaces = filterValues(namespaces);
			List<String> filteredArtifactTypes = filterValues(artifactTypes);
			List<String> filteredArtifactCategories = filterValues(artifactCategories);
			if (namespaces != null && filteredNamespaces.isEmpty()) {
				return Collections.emptyList();
			}
			boolean hasWhere = false;
			if (!filteredNamespaces.isEmpty()) {
				sql.append(" where (");
				for (int i = 0; i < filteredNamespaces.size(); i++) {
					if (i > 0) {
						sql.append(" or ");
					}
					sql.append("artifact_id = ? or artifact_id like ?");
				}
				sql.append(")");
				hasWhere = true;
			}
			if (!filteredGlobs.isEmpty()) {
				sql.append(hasWhere ? " and (" : " where (");
				for (int i = 0; i < filteredGlobs.size(); i++) {
					if (i > 0) {
						sql.append(" or ");
					}
					sql.append("path like ? escape '\\\\'");
				}
				sql.append(")");
				hasWhere = true;
			}
			if (!filteredArtifactTypes.isEmpty()) {
				sql.append(hasWhere ? " and (" : " where (");
				for (int i = 0; i < filteredArtifactTypes.size(); i++) {
					if (i > 0) {
						sql.append(" or ");
					}
					sql.append("artifact_type = ?");
				}
				sql.append(")");
				hasWhere = true;
			}
			if (!filteredArtifactCategories.isEmpty()) {
				sql.append(hasWhere ? " and (" : " where (");
				for (int i = 0; i < filteredArtifactCategories.size(); i++) {
					if (i > 0) {
						sql.append(" or ");
					}
					sql.append("artifact_category = ?");
				}
				sql.append(")");
			}
			sql.append(" order by artifact_id, path");
			statement = connection.prepareStatement(sql.toString());
			int parameter = 1;
			for (String namespace : filteredNamespaces) {
				statement.setString(parameter++, namespace);
				statement.setString(parameter++, namespace + ".%");
			}
			for (String glob : filteredGlobs) {
				statement.setString(parameter++, toSqlLike(glob));
			}
			for (String artifactType : filteredArtifactTypes) {
				statement.setString(parameter++, artifactType);
			}
			for (String artifactCategory : filteredArtifactCategories) {
				statement.setString(parameter++, artifactCategory);
			}
			resultSet = statement.executeQuery();
			List<FragmentSearch> results = new ArrayList<FragmentSearch>();
			while (resultSet.next()) {
				String content = resultSet.getString("content");
				List<String> matches = RipgrepFormatter.format(content, compiled, before, after);
				if (!matches.isEmpty()) {
					results.add(mapFragment(resultSet, matches, content));
					if (limit > 0 && results.size() >= limit) {
						break;
					}
				}
			}
			return results;
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
		finally {
			close(resultSet);
			close(statement);
			close(connection);
		}
	}

	private void indexBatch(Connection connection, PreparedStatement upsert, String artifactId, String artifactType, String artifactCategory, long version, List<ArtifactFragment> fragments, boolean batchCommit) throws SQLException {
		int pending = 0;
		for (ArtifactFragment fragment : fragments) {
			String hash = hash(fragment.getContent());
			upsert.setString(1, artifactId);
			upsert.setLong(2, version);
			upsert.setString(3, fragment.getPath());
			upsert.setString(4, artifactType);
			upsert.setString(5, artifactCategory);
			upsert.setString(6, fragment.getFragmentType());
			upsert.setString(7, fragment.getContentType());
			upsert.setString(8, fragment.getContent());
			upsert.setString(9, serializeProperties(fragment.getProperties()));
			upsert.setString(10, hash);
			upsert.setBoolean(11, fragment.isEditable());
			upsert.setBoolean(12, fragment.isRemovable());
			upsert.addBatch();
			pending++;
			if (pending >= BATCH_SIZE) {
				upsert.executeBatch();
				pending = 0;
				if (batchCommit) {
					connection.commit();
				}
			}
		}
		if (pending > 0) {
			upsert.executeBatch();
		}
	}

	private void deleteMissing(Connection connection, String artifactId, List<ArtifactFragment> fragments) throws SQLException {
		PreparedStatement delete = null;
		try {
			if (fragments.isEmpty()) {
				delete = connection.prepareStatement("delete from fragment_index where artifact_id = ?");
				delete.setString(1, artifactId);
			}
			else {
				delete = connection.prepareStatement("delete from fragment_index where artifact_id = ? and path not in (" + placeholders(fragments.size()) + ")");
				delete.setString(1, artifactId);
				for (int i = 0; i < fragments.size(); i++) {
					delete.setString(i + 2, fragments.get(i).getPath());
				}
			}
			delete.executeUpdate();
		}
		finally {
			close(delete);
		}
	}

	private FragmentSearch mapFragment(ResultSet resultSet, List<String> matches) throws SQLException {
		return mapFragment(resultSet, matches, resultSet.getString("content"));
	}

	private FragmentSearch mapFragment(ResultSet resultSet, List<String> matches, String content) throws SQLException {
		return new FragmentSearch(resultSet.getString("artifact_id"), resultSet.getString("path"), resultSet.getString("artifact_type"), resultSet.getString("artifact_category"), resultSet.getString("fragment_type"), content, resultSet.getString("content_type"), deserializeProperties(resultSet.getString("properties")), matches, resultSet.getBoolean("editable"), resultSet.getBoolean("removable"));
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

	private String toSqlLike(String glob) {
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

	private String placeholders(int amount) {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < amount; i++) {
			if (i > 0) {
				builder.append(", ");
			}
			builder.append("?");
		}
		return builder.toString();
	}

	private String hash(String content) {
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

	private String serializeProperties(Map<String, String> properties) {
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

	private Map<String, String> deserializeProperties(String serialized) {
		if (serialized == null || serialized.isEmpty()) {
			return Collections.emptyMap();
		}
		java.util.LinkedHashMap<String, String> properties = new java.util.LinkedHashMap<String, String>();
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


	private void rollback(Connection connection) {
		if (connection != null) {
			try {
				connection.rollback();
			}
			catch (SQLException e) {
				logger.error("Could not rollback fragment index transaction", e);
			}
		}
	}

	private void resetAutoCommit(Connection connection) {
		if (connection != null) {
			try {
				connection.setAutoCommit(true);
			}
			catch (SQLException e) {
				logger.error("Could not reset auto-commit on fragment index connection", e);
			}
		}
	}

	private void close(AutoCloseable closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			}
			catch (Exception e) {
				logger.error("Could not close fragment index resource", e);
			}
		}
	}
}
