package be.nabu.eai.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.io.BufferedInputStream;

import be.nabu.eai.repository.EAIResourceRepository;
import be.nabu.libs.authentication.api.RoleHandler;
import be.nabu.libs.events.impl.EventDispatcherImpl;
import be.nabu.libs.http.api.server.HTTPServer;
import be.nabu.libs.http.server.HTTPServerUtils;
import be.nabu.libs.resources.ResourceFactory;
import be.nabu.libs.resources.URIUtils;
import be.nabu.libs.resources.api.ResourceContainer;

public class Standalone {
	
	public static void main(String...args) throws IOException, URISyntaxException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		String propertiesFileName = getArgument("properties", "server.properties", args);
		File propertiesFile = new File(propertiesFileName);
		if (propertiesFile.exists()) {
			Properties properties = new Properties();
			InputStream input = new BufferedInputStream(new FileInputStream(propertiesFile));
			try {
				properties.load(input);
			}
			finally {
				input.close();
			}
			System.getProperties().putAll(properties);
		}
		
		int port = new Integer(getArgument("port", "5555", args));
		int listenerPoolSize = new Integer(getArgument("listenerPoolSize", "20", args));

		URI maven = new URI(URIUtils.encodeURI(getMandatoryArgument("maven", args)));
		ResourceContainer<?> mavenRoot = (ResourceContainer<?>) ResourceFactory.getInstance().resolve(maven, null);
		if (mavenRoot == null) {
			throw new IOException("The directory for the maven repository does not exist: " + maven);
		}
		
		URI repository = new URI(URIUtils.encodeURI(getMandatoryArgument("repository", args)));
		ResourceContainer<?> repositoryRoot = (ResourceContainer<?>) ResourceFactory.getInstance().resolve(repository, null);
		if (repositoryRoot == null) {
			throw new IOException("The directory for the repository does not exist: " + repository);
		}
		
		RoleHandler roleHandler = null;
		if (getArgument("roles", null, args) != null) {
			roleHandler = (RoleHandler) Class.forName(getArgument("roles", null, args)).newInstance();	
		}

		boolean enableREST = new Boolean(getArgument("enableREST", "false", args));
		boolean enableMaven = new Boolean(getArgument("enableMaven", "false", args));
		boolean enableRepository = new Boolean(getArgument("enableRepository", Boolean.toString(enableREST), args));
		boolean forceRemoteRepository = new Boolean(getArgument("forceRemoteRepository", "false", args));
		boolean updateMavenSnapshots = new Boolean(getArgument("updateMavenSnapshots", "false", args));
		boolean enableMetrics = new Boolean(getArgument("enableMetrics", "true", args));
		boolean anonymousIsRoot = new Boolean(getArgument("anonymousIsRoot", "true", args));
		String authenticationService = getArgument("authentication", null, args);
		String roleService = getArgument("role", null, args);
		
		String localMavenServer = getArgument("localMavenServer", null, args);
		String serverName = getArgument("name", null, args);
		
		// create the repository
		EAIResourceRepository repositoryInstance = new EAIResourceRepository(repositoryRoot, mavenRoot);
		repositoryInstance.enableMetrics(enableMetrics);
		
		// create the server
		Server server = new Server(roleHandler, repositoryInstance);
		server.setName(serverName);
		// set the server as the runner for the repository
		repositoryInstance.setServiceRunner(server);
		
		if (localMavenServer != null) {
			repositoryInstance.setLocalMavenServer(new URI(URIUtils.encodeURI(localMavenServer)));
			repositoryInstance.setUpdateMavenSnapshots(updateMavenSnapshots);
		}
		server.setAnonymousIsRoot(anonymousIsRoot);
		server.start();
		
		if (enableREST || enableMaven || enableRepository) {
			HTTPServer http = HTTPServerUtils.newServer(port, listenerPoolSize, new EventDispatcherImpl());
			if (authenticationService != null) {
				if (!server.enableSecurity(http, authenticationService, roleService)) {
					return;
				}
			}
			if (enableREST) {
				server.enableREST(http);
			}
			if (enableRepository) {
				server.enableRepository(http);
				server.setForceRemoteRepository(forceRemoteRepository);
			}
			if (enableMaven) {
				server.enableMaven(http);
			}
			http.start();
		}
	}
	
	public static String getArgument(String name, String defaultValue, String...args) {
		for (String argument : args) {
			if (argument.startsWith(name + "=")) {
				String value = argument.substring(name.length() + 1);
				return value.isEmpty() ? null : value;
			}
		}
		return System.getProperty(Standalone.class.getName() + "." + name, defaultValue);
	}
	
	public static String getMandatoryArgument(String name, String...args) {
		String value = getArgument(name, null, args);
		if (value == null) {
			throw new IllegalArgumentException("Missing mandatory argument " + name);
		}
		return value;
	}
}
