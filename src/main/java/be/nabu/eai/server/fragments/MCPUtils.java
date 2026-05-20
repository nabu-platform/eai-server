package be.nabu.eai.server.fragments;

import java.io.File;
import java.nio.file.Path;

public final class MCPUtils {

	private MCPUtils() {
		// utility
	}

	public static Path getMcpRootPath() {
		String folder = System.getProperty(FileSystemFragmentIndexBackend.MCP_PATH);
		if (folder == null || folder.trim().isEmpty()) {
			String property = System.getProperty("user.home");
			File target = property == null ? new File(".") : new File(property);
			return new File(target, ".nabu").toPath().toAbsolutePath();
		}
		return new File(folder).toPath().toAbsolutePath();
	}

	public static Path getFragmentsPath() {
		return getMcpRootPath().resolve("fragments");
	}

	public static Path getTracesPath() {
		return getMcpRootPath().resolve("traces");
	}
}
