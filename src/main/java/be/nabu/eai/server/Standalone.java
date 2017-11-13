package be.nabu.eai.server;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.eai.repository.EAIResourceRepository;
import be.nabu.eai.repository.api.LicenseManager;
import be.nabu.eai.repository.util.LicenseManagerImpl;
import be.nabu.eai.repository.util.SystemPrincipal;
import be.nabu.libs.artifacts.api.Artifact;
import be.nabu.libs.authentication.api.PermissionHandler;
import be.nabu.libs.authentication.api.RoleHandler;
import be.nabu.libs.resources.ResourceFactory;
import be.nabu.libs.resources.ResourceUtils;
import be.nabu.libs.resources.URIUtils;
import be.nabu.libs.resources.alias.AliasResourceResolver;
import be.nabu.libs.resources.api.ReadableResource;
import be.nabu.libs.resources.api.Resource;
import be.nabu.libs.resources.api.ResourceContainer;
import be.nabu.libs.resources.file.FileDirectory;
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

		ResourceContainer<?> mavenRoot = null;
		String modulesUri = getArgument("modules", getArgument("maven", null, args), args);
		if (modulesUri != null) {
			URI maven = new URI(URIUtils.encodeURI(modulesUri));
			mavenRoot = (ResourceContainer<?>) ResourceFactory.getInstance().resolve(maven, null);
			if (mavenRoot == null) {
				throw new IOException("The directory for the maven repository does not exist: " + maven);
			}
		}
		
		URI repository = new URI(URIUtils.encodeURI(getMandatoryArgument("repository", args)));
		ResourceContainer<?> repositoryRoot = (ResourceContainer<?>) ResourceFactory.getInstance().resolve(repository, null);
		if (repositoryRoot == null) {
			throw new IOException("The directory for the repository does not exist: " + repository);
		}
		
		LicenseManager licenseManager = new LicenseManagerImpl();
		String licenseFolder = getArgument("licenses", null, args);
		if (licenseFolder != null) {
			URI licenses = new URI(URIUtils.encodeURI(licenseFolder));
			ResourceContainer<?> licenseRoot = licenses == null ? null : (ResourceContainer<?>) ResourceFactory.getInstance().resolve(licenses, null);
			if (licenseRoot == null) {
				throw new RuntimeException("The configured license folder is invalid: " + licenseFolder);
			}
			else {
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
		
		ResourceContainer<?> deploymentRoot;
		String deploymentFolder = getArgument("deployments", null, args);
		if (deploymentFolder != null) {
			URI deployments = new URI(URIUtils.encodeURI(deploymentFolder));
			deploymentRoot = ResourceUtils.mkdir(deployments, null);
		}
		else {
			File file = new File("deployments");
			if (!file.exists()) {
				file.mkdir();
			}
			deploymentRoot = new FileDirectory(null, file, false);
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

		if (roleService != null) {
			Artifact resolve = repositoryInstance.resolve(roleService);
			if (resolve == null) {
				logger.error("Invalid role service: " + roleService);
			}
			else {
				roleHandler = POJOUtils.newProxy(RoleHandler.class, repositoryInstance, SystemPrincipal.ROOT, (DefinedService) resolve);
				repositoryInstance.setRoleHandler(roleHandler);
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
		
		// create the server
		Server server = new Server(roleHandler, repositoryInstance);
		server.setDeployments(deploymentRoot);
		server.setEnableSnapshots(enableSnapshots);
		// set the server as the runner for the repository
		repositoryInstance.setServiceRunner(server);
		
		if (localMavenServer != null) {
			repositoryInstance.setLocalMavenServer(new URI(URIUtils.encodeURI(localMavenServer)));
			repositoryInstance.setUpdateMavenSnapshots(updateMavenSnapshots);
		}
		server.setAnonymousIsRoot(anonymousIsRoot);
		server.setListenerPoolSize(listenerPoolSize);
		server.setPort(port);
		server.start();

		String loggerService = getArgument("logger", null, args);
		if (loggerService != null) {
			server.enableLogger(loggerService);
		}

		if (enableREST || enableMaven || enableRepository) {
			if (authenticationService != null) {
				if (!server.enableSecurity(authenticationService)) {
					logger.error("Could not enable security, the http server will not be started");
					return;
				}
			}
			if (enableREST) {
				server.enableREST();
			}
			if (enableRepository) {
				server.enableRepository();
				server.setForceRemoteRepository(forceRemoteRepository);
			}
			Map<String, URI> aliases = AliasResourceResolver.getAliases();
			for (String alias : aliases.keySet()) {
				boolean enableAlias = new Boolean(getArgument("enableAlias." + alias, "false", args));
				if (enableAlias) {
					server.enableAlias(alias, aliases.get(alias));
				}
			}
			if (enableMaven) {
				server.enableMaven();
			}
		}
		if (server.hasHTTPServer()) {
			server.getHTTPServer().start();
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
