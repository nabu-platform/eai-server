package be.nabu.eai.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.io.BufferedInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.eai.repository.EAIResourceRepository;
import be.nabu.eai.repository.api.LicenseManager;
import be.nabu.eai.repository.util.LicenseManagerImpl;
import be.nabu.eai.repository.util.SystemPrincipal;
import be.nabu.libs.artifacts.api.Artifact;
import be.nabu.libs.authentication.api.PermissionHandler;
import be.nabu.libs.authentication.api.RoleHandler;
import be.nabu.libs.events.impl.EventDispatcherImpl;
import be.nabu.libs.http.api.server.HTTPServer;
import be.nabu.libs.http.server.HTTPServerUtils;
import be.nabu.libs.resources.ResourceFactory;
import be.nabu.libs.resources.URIUtils;
import be.nabu.libs.resources.api.ReadableResource;
import be.nabu.libs.resources.api.Resource;
import be.nabu.libs.resources.api.ResourceContainer;
import be.nabu.libs.resources.snapshot.SnapshotUtils;
import be.nabu.libs.services.api.DefinedService;
import be.nabu.libs.services.pojo.POJOUtils;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.ReadableContainer;
import be.nabu.utils.security.SecurityUtils;

public class Standalone {
	
	private static Logger logger = LoggerFactory.getLogger(Standalone.class);
	
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

		String modulesUri = getArgument("modules", getArgument("maven", null, args), args);
		if (modulesUri == null) {
			throw new IllegalArgumentException("Could not find modules folder");
		}
		URI maven = new URI(URIUtils.encodeURI(modulesUri));
		ResourceContainer<?> mavenRoot = (ResourceContainer<?>) ResourceFactory.getInstance().resolve(maven, null);
		if (mavenRoot == null) {
			throw new IOException("The directory for the maven repository does not exist: " + maven);
		}
		
		URI repository = new URI(URIUtils.encodeURI(getMandatoryArgument("repository", args)));
		ResourceContainer<?> repositoryRoot = (ResourceContainer<?>) ResourceFactory.getInstance().resolve(repository, null);
		if (repositoryRoot == null) {
			throw new IOException("The directory for the repository does not exist: " + repository);
		}
		
		LicenseManager licenseManager = null;
		String licenseFolder = getArgument("licenses", null, args);
		if (licenseFolder != null) {
			URI licenses = new URI(URIUtils.encodeURI(licenseFolder));
			ResourceContainer<?> licenseRoot = licenses == null ? null : (ResourceContainer<?>) ResourceFactory.getInstance().resolve(licenses, null);
			if (licenseRoot == null) {
				throw new RuntimeException("The configured license folder is invalid: " + licenseFolder);
			}
			else {
				licenseManager = new LicenseManagerImpl();
				for (Resource resource : licenseRoot) {
					if (resource instanceof ReadableResource) {
						try {
							ReadableContainer<ByteBuffer> readable = ((ReadableResource) resource).getReadable();
							try {
								licenseManager.addLicense(SecurityUtils.parseCertificate(IOUtils.toInputStream(readable)));
							}
							finally {
								readable.close();
							}
						}
						catch (Exception e) {
							logger.warn("Ignoring file in licensing folder: " + resource.getName(), e);
						}
					}
				}
			}
		}
		else {
			logger.warn("No license folder configured for this server");
		}
		
		RoleHandler roleHandler = null;
		if (getArgument("roles", null, args) != null) {
			roleHandler = (RoleHandler) Class.forName(getArgument("roles", null, args)).newInstance();	
		}

		boolean enableSnapshots = new Boolean(getArgument("enableSnapshots", "false", args));
		boolean enableREST = new Boolean(getArgument("enableREST", "false", args));
		boolean enableMaven = new Boolean(getArgument("enableMaven", "false", args));
		boolean enableRepository = new Boolean(getArgument("enableRepository", Boolean.toString(enableREST), args));
		boolean forceRemoteRepository = new Boolean(getArgument("forceRemoteRepository", "false", args));
		boolean updateMavenSnapshots = new Boolean(getArgument("updateMavenSnapshots", "false", args));
		boolean enableMetrics = new Boolean(getArgument("enableMetrics", "true", args));
		boolean historizeGauges = new Boolean(getArgument("historizeGauges", Boolean.toString(enableMetrics), args));
		boolean anonymousIsRoot = new Boolean(getArgument("anonymousIsRoot", "true", args));
		long historizationInterval = Long.parseLong(getArgument("historizationInterval", "5000", args));
		int historySize = Integer.parseInt(getArgument("historySize", "1000", args));
		
		String authenticationService = getArgument("authentication", null, args);
		String roleService = getArgument("role", null, args);
		String permissionService = getArgument("permission", null, args);
		
		String localMavenServer = getArgument("localMavenServer", null, args);
		String serverName = getArgument("name", null, args);
		String groupName = getArgument("group", null, args);
		
		// create the repository
		EAIResourceRepository repositoryInstance = new EAIResourceRepository(enableSnapshots ? SnapshotUtils.prepare(repositoryRoot) : repositoryRoot, mavenRoot);
		repositoryInstance.setHistorizeGauges(historizeGauges);
		repositoryInstance.enableMetrics(enableMetrics);
		repositoryInstance.setName(serverName);
		repositoryInstance.setGroup(groupName == null ? serverName : groupName);
		repositoryInstance.setLicenseManager(licenseManager);
		repositoryInstance.setHistorizationInterval(historizationInterval);
		repositoryInstance.setHistorySize(historySize);
		
		// create the server
		Server server = new Server(roleHandler, repositoryInstance);
		server.setEnableSnapshots(enableSnapshots);
		// set the server as the runner for the repository
		repositoryInstance.setServiceRunner(server);
		
		if (localMavenServer != null) {
			repositoryInstance.setLocalMavenServer(new URI(URIUtils.encodeURI(localMavenServer)));
			repositoryInstance.setUpdateMavenSnapshots(updateMavenSnapshots);
		}
		server.setAnonymousIsRoot(anonymousIsRoot);
		server.setPort(port);
		server.start();

		if (roleService != null) {
			Artifact resolve = repositoryInstance.resolve(roleService);
			if (resolve == null) {
				logger.error("Invalid role service: " + roleService);
			}
			else {
				repositoryInstance.setRoleHandler(POJOUtils.newProxy(RoleHandler.class, repositoryInstance, SystemPrincipal.ROOT, (DefinedService) resolve));
			}
		}
		
		if (permissionService != null) {
			Artifact resolve = repositoryInstance.resolve(permissionService);
			if (resolve == null) {
				logger.error("Invalid permission service: " + permissionService);
			}
			else {
				repositoryInstance.setPermissionHandler(POJOUtils.newProxy(PermissionHandler.class, repositoryInstance, SystemPrincipal.ROOT, (DefinedService) resolve));
			}
		}

		if (enableREST || enableMaven || enableRepository) {
			HTTPServer http = HTTPServerUtils.newServer(port, listenerPoolSize, new EventDispatcherImpl());
			if (authenticationService != null) {
				if (!server.enableSecurity(http, authenticationService, roleService)) {
					logger.error("Could not enable security, the http server will not be started");
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
