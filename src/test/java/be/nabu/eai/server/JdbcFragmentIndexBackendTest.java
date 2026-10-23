package be.nabu.eai.server;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.junit.Test;
import static org.junit.Assert.*;

import be.nabu.eai.repository.api.ArtifactFragmentManager.ArtifactFragment;
import be.nabu.eai.server.fragments.JdbcFragmentIndexBackend;

public class JdbcFragmentIndexBackendTest {
	private ArtifactFragment fragment(String path, String content) {
		return new ArtifactFragment() {
			public String getPath() { return path; }
			public String getContent() { return content; }
			public String getContentType() { return "text/plain"; }
			public String getArtifactId() { return "app"; }
			public String getFragmentType() { return "resource"; }
			public java.util.Map<String, String> getProperties() { return Collections.emptyMap(); }
			public boolean isEditable() { return false; }
			public boolean isRemovable() { return false; }
		};
	}

	@Test
	public void rebuildCommitsCompleteManifestAndAbortPreservesPreviousIndex() {
		String url = "jdbc:h2:mem:fragment-index-" + System.nanoTime() + ";DB_CLOSE_DELAY=-1";
		DataSource source = new DataSource() {
			public Connection getConnection() throws SQLException { return DriverManager.getConnection(url); }
			public Connection getConnection(String username, String password) throws SQLException { return getConnection(); }
			public PrintWriter getLogWriter() { return null; }
			public void setLogWriter(PrintWriter writer) { }
			public void setLoginTimeout(int seconds) { }
			public int getLoginTimeout() { return 0; }
			public Logger getParentLogger() { return Logger.getGlobal(); }
			public <T> T unwrap(Class<T> type) throws SQLException { throw new SQLException("unsupported"); }
			public boolean isWrapperFor(Class<?> type) { return false; }
		};
		JdbcFragmentIndexBackend backend = new JdbcFragmentIndexBackend(source);
		backend.initialize();
		backend.index("app", "item", "category", 1, Arrays.asList(fragment("old", "old"), fragment("keep", "keep")), Arrays.asList("old", "keep"));
		backend.beginRebuild();
		backend.index("app", "item", "category", 2, Collections.singletonList(fragment("new", "new")), Collections.singletonList("new"));
		assertNotNull(backend.get("app", "old"));
		assertNull(backend.get("app", "new"));
		backend.abortRebuild();
		assertNotNull(backend.get("app", "keep"));
		backend.beginRebuild();
		java.util.List<ArtifactFragment> batch = new java.util.ArrayList<ArtifactFragment>();
		java.util.List<String> batchPaths = new java.util.ArrayList<String>();
		for (int i = 0; i < 520; i++) {
			batch.add(fragment("batch" + i, "value"));
			batchPaths.add("batch" + i);
		}
		backend.index("app", "item", "category", 2, batch, batchPaths);
		backend.abortRebuild();
		assertNull(backend.get("app", "batch500"));
		assertNotNull(backend.get("app", "old"));
		backend.beginRebuild();
		backend.index("app", "item", "category", 3, Collections.singletonList(fragment("new", "new")), Collections.singletonList("new"));
		backend.finalizeRebuild();
		assertNull(backend.get("app", "old"));
		assertNull(backend.get("app", "keep"));
		assertEquals("new", backend.get("app", "new").getContent());
		backend.index("app", "item", "category", 4, Collections.singletonList(fragment("new", "updated")), Collections.singletonList("new"));
		assertEquals("updated", backend.get("app", "new").getContent());
	}
}
