package be.nabu.eai.server;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import be.nabu.eai.repository.EAIResourceRepository;
import be.nabu.eai.repository.RepositoryThreadFactory;
import be.nabu.eai.repository.api.LicenseManager;
import be.nabu.eai.repository.util.LicenseManagerImpl;
import be.nabu.eai.repository.util.SystemPrincipal;
import be.nabu.libs.artifacts.api.Artifact;
import be.nabu.libs.authentication.api.PermissionHandler;
import be.nabu.libs.authentication.api.RoleHandler;
import be.nabu.libs.cluster.hazelcast.HazelcastClusterInstance;
import be.nabu.libs.cluster.local.LocalInstance;
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
import be.nabu.utils.cep.api.EventSeverity;
import be.nabu.utils.cep.impl.ComplexEventImpl;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.ReadableContainer;
import be.nabu.utils.security.SecurityUtils;

public class Standalone {
	
	private static Logger logger = LoggerFactory.getLogger(Standalone.class);
	private Server server;
	
	public static void main(String...args) throws IOException, URISyntaxException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		// ask to stop
		if (args.length > 0 && args[0].equalsIgnoreCase("stop")) {
			System.exit(0);
		}
		Standalone alone = new Standalone();
		alone.initialize(args);
		alone.start();
	}

	public void start() throws IOException {
		if (server.hasHTTPServer()) {
			server.getHTTPServer().start();
		}
	}
	public void initialize(String...args) throws FileNotFoundException, IOException, URISyntaxException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		ComplexEventImpl startupEvent = new ComplexEventImpl();
		startupEvent.setCode("STARTUP");
		startupEvent.setEventName("nabu-server-start");
		startupEvent.setStarted(new Date());
		startupEvent.setCreated(startupEvent.getStarted());
		startupEvent.setSeverity(EventSeverity.INFO);
		
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
		
		String repositoryString = getMandatoryArgument("repository", args);
		// if absolute path but no scheme, use file
		if (repositoryString.startsWith("/")) {
			repositoryString = "file:" + repositoryString;
		}
		// if no scheme and no absolute path, we assume file relative to the current directory
		else if (!repositoryString.matches("^[\\w]+:/.*")) {
			repositoryString = "file:" + new File(repositoryString).getCanonicalPath();
		}
		logger.info("Repository located at: " + repositoryString);
		URI repository = new URI(URIUtils.encodeURI(repositoryString));
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

		String cluster = getArgument("cluster", "false", args);
		// if snapshots are disabled, deployments "can" go wrong because this happens:
		// clean folders
		// reload everything
		// for some reason the server reports there is nothing (should still see the stuff in memory but doesn't)
		// so nothing gets unloaded but everything does get loaded
		// this means stuff like http servers etc fail on restart because the previous one is still running
		// should definitely fix the deployment procedure as well but enabling snapshots in non-development environments is not a bad thing either
		boolean enableSnapshots = new Boolean(getArgument("enableSnapshots", Boolean.toString(!EAIResourceRepository.isDevelopment()), args));
		boolean enableREST = new Boolean(getArgument("enableREST", "false", args));
		boolean enableMaven = new Boolean(getArgument("enableMaven", "false", args));
		boolean enableRepository = new Boolean(getArgument("enableRepository", Boolean.toString(enableREST), args));
		boolean forceRemoteRepository = new Boolean(getArgument("forceRemoteRepository", "true", args));
		boolean updateMavenSnapshots = new Boolean(getArgument("updateMavenSnapshots", "false", args));
		boolean enableMetrics = new Boolean(getArgument("enableMetrics", "true", args));
		boolean historizeGauges = new Boolean(getArgument("historizeGauges", Boolean.toString(enableMetrics), args));
		boolean anonymousIsRoot = new Boolean(getArgument("anonymousIsRoot", "true", args));
		boolean startup = new Boolean(getArgument("startup", "true", args));
		boolean logComplexEvents = new Boolean(getArgument("logComplexEvents", "true", args));
		String cepService = getArgument("cepService", null, args);
		long historizationInterval = Long.parseLong(getArgument("historizationInterval", "5000", args));
		int historySize = Integer.parseInt(getArgument("historySize", "1000", args));
		
		int pool = Integer.parseInt(getArgument("pool", "" + Runtime.getRuntime().availableProcessors(), args));
		
		String authenticationService = getArgument("authentication", null, args);
		String roleService = getArgument("role", null, args);
		String permissionService = getArgument("permission", null, args);
		
		String localMavenServer = getArgument("localMavenServer", null, args);
		String serverName = getArgument("name", null, args);
		String groupName = getArgument("group", null, args);
		String aliasName = getArgument("alias", null, args);
		
		// create the repository
		EAIResourceRepository repositoryInstance = new EAIResourceRepository(enableSnapshots ? SnapshotUtils.prepare(repositoryRoot) : repositoryRoot, mavenRoot);
		repositoryInstance.setHistorizeGauges(historizeGauges);
		repositoryInstance.enableMetrics(enableMetrics);
		repositoryInstance.setName(serverName);
		repositoryInstance.setGroup(groupName == null ? serverName : groupName);
		repositoryInstance.setLicenseManager(licenseManager);
		repositoryInstance.setHistorizationInterval(historizationInterval);
		repositoryInstance.setHistorySize(historySize);
		
		if (aliasName != null) {
			repositoryInstance.getAliases().addAll(Arrays.asList(aliasName.split("[\\s]*,[\\s]*")));
		}

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
		
		server = new Server(roleHandler, repositoryInstance, startupEvent);
		
		if (!startup) {
			server.setDisableStartup(true);
		}
		server.setDeployments(deploymentRoot);
		server.setEnableSnapshots(enableSnapshots);
		// make sure we also use the correct pool here, otherwise the thread context is wrong and we might not be able to access libraries available in the repository
		// we had this with an invoke $all to bringOnline where startup listeners failed to for example find the sftp library, the jdbc pool artifact etc etc
		server.setPool(Executors.newFixedThreadPool(pool, new RepositoryThreadFactory(repositoryInstance)));
		// set the server as the runner for the repository
		repositoryInstance.setServiceRunner(server);
		
		// if it is not set to false, check if it is either true (use default settings) or points to a file
		if (cluster != null && !cluster.equalsIgnoreCase("false")) {
			
			// hazelcast calls home once a day, it is not entirely clear _why_ but it sends information about the cluster
			// I dislike this kind of practice out of principle so I rather disable it by default
			// there is a system parameter for it as can be seen in the code:
			// https://github.com/hazelcast/hazelcast/blob/master/hazelcast/src/main/java/com/hazelcast/internal/util/PhoneHome.java
			System.setProperty("HZ_PHONE_HOME_ENABLED", "false");
			
			Config config;
			// just use the default configuration
			if (cluster.equalsIgnoreCase("true")) {
				config = new Config();
			}
			// load from file system
			else {
				Resource resolve = ResourceFactory.getInstance().resolve(new URI(URIUtils.encodeURI(cluster)), null);
				if (resolve == null) {
					throw new FileNotFoundException("Can not find hazelcast configuration: " + cluster);
				}
				ReadableContainer<ByteBuffer> readable = ((ReadableResource) resolve).getReadable();
				try {
					config = new XmlConfigBuilder(IOUtils.toInputStream(readable)).build();
				}
				finally {
					readable.close();
				}
			}
			config.getMemberAttributeConfig().setStringAttribute("group", repositoryInstance.getGroup());
			config.getMemberAttributeConfig().setStringAttribute("name", repositoryInstance.getName());
			config.setClassLoader(repositoryInstance.getClassLoader());
	        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
			server.setCluster(new HazelcastClusterInstance(instance));
		}
		else {
			server.setCluster(new LocalInstance());
		}
		
		if (logComplexEvents) {
			repositoryInstance.getComplexEventDispatcher().subscribe(Object.class, new CEFLogger(server));
		}
		MultipleCEPProcessor cepProcessor = new MultipleCEPProcessor(server);
		server.setProcessor(cepProcessor);
		// if we have registered one...add it (partly for backwards compatibility)
		if (cepService != null) {
			for (String singleService : cepService.split(",")) {
				cepProcessor.add(singleService.trim());
			}
		}
		repositoryInstance.getComplexEventDispatcher().subscribe(Object.class, cepProcessor);
		
		server.initialize();
		
		if (localMavenServer != null) {
			repositoryInstance.setLocalMavenServer(new URI(URIUtils.encodeURI(localMavenServer)));
			repositoryInstance.setUpdateMavenSnapshots(updateMavenSnapshots);
		}
		server.setAnonymousIsRoot(anonymousIsRoot);
		server.setListenerPoolSize(listenerPoolSize);
		server.setPort(port);
		server.start();

		String loggerService = getArgument("logger", null, args);
		String loggerAsync = getArgument("loggerAsync", null, args);
		if (loggerService != null) {
			server.enableLogger(loggerService, loggerAsync == null || !loggerAsync.equalsIgnoreCase("false"), getChildren("logger", args));
		}

		if (enableREST || enableMaven || enableRepository) {
			if (!server.enableSecurity(authenticationService)) {
				logger.error("Could not enable security, the http server will not be started");
				return;
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
	}
	
	public static String getArgument(String name, String defaultValue, String...args) {
		for (String argument : args) {
			if (argument.startsWith(name + "=")) {
				String value = argument.substring(name.length() + 1);
				return value.isEmpty() ? null : value;
			}
		}
		return System.getProperty(Standalone.class.getName() + "." + name, System.getProperty(name, defaultValue));
	}
	
	public static Map<String, String> getChildren(String name, String...args) {
		Map<String, String> properties = new HashMap<String, String>();
		for (String argument : args) {
			if (argument.startsWith(name + ".")) {
				int index = argument.indexOf('=');
				if (index > 0) {
					properties.put(argument.substring(name.length() + 1, index), argument.substring(index + 1));
				}
			}
		}
		for (Object key : System.getProperties().keySet()) {
			String keyName = key.toString();
			if (keyName.startsWith(name + ".")) {
				properties.put(keyName.substring(name.length() + 1), System.getProperty(keyName));
			}
		}
		return properties;
	}
	
	public static String getMandatoryArgument(String name, String...args) {
		String value = getArgument(name, null, args);
		if (value == null) {
			throw new IllegalArgumentException("Missing mandatory argument " + name);
		}
		return value;
	}
}
