package be.nabu.eai.server;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
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
import be.nabu.eai.repository.impl.AuthenticationEnricher;
import be.nabu.eai.repository.impl.CorrelationIdEnricher;
import be.nabu.eai.repository.impl.CreatedDateEnricher;
import be.nabu.eai.repository.util.LicenseManagerImpl;
import be.nabu.eai.repository.util.MetricStatistics;
import be.nabu.eai.repository.util.SystemPrincipal;
import be.nabu.libs.artifacts.api.Artifact;
import be.nabu.libs.authentication.api.PermissionHandler;
import be.nabu.libs.authentication.api.RoleHandler;
import be.nabu.libs.cluster.hazelcast.HazelcastClusterInstance;
import be.nabu.libs.cluster.local.LocalInstance;
import be.nabu.libs.resources.ResourceFactory;
import be.nabu.libs.resources.ResourceReadableContainer;
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
		try {
			logger.debug("Initializing the server...");
			alone.initialize(args);
			logger.debug("Starting the server...");
			alone.start();
		}
		catch (Exception e) {
			logger.error("Could not start server", e);
			throw new RuntimeException(e);
		}
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
		
		String repositoryString = getArgument("repository", null, args);
		// we might not have an absolute repository position, but a relative one
		// we often package the integrator and repository together as a runnable solution
		// the current running directory is often not a good match for finding that location
		// however, we can use the location of for example the jar file that contains this class as an absolute to where the lib folder is located
		if (repositoryString == null) {
			String relativeRepositoryString = getArgument("relativeRepository", null, args);
			if (relativeRepositoryString == null) {
				throw new IllegalArgumentException("Missing repository location configuration");
			}
			String asciiString = getIntegratorPath();
			logger.info("Resolving relative repository to location: " + asciiString);
			repositoryString = asciiString + "/" + relativeRepositoryString;
			repositoryString = URIUtils.normalize(repositoryString);
			logger.info("Final repository path: " + repositoryString);
		}
		
		// if absolute path but no scheme, use file
		if (repositoryString.startsWith("/")) {
			repositoryString = "file:" + repositoryString;
		}
		// if no scheme and no absolute path, we assume file relative to the current directory
		else if (!repositoryString.matches("^[\\w]+:/.*")) {
			String asciiString = getIntegratorPath();
			logger.info("Resolving relative repository to location: " + asciiString);
			repositoryString = asciiString + "/" + repositoryString;
			repositoryString = URIUtils.normalize(repositoryString);
			logger.info("Final repository path: " + repositoryString);
//			repositoryString = "file:" + new File(repositoryString).getCanonicalPath();
		}
		logger.info("Repository located at: " + repositoryString);
		URI repository = new URI(URIUtils.encodeURI(repositoryString));
		ResourceContainer<?> repositoryRoot = (ResourceContainer<?>) ResourceFactory.getInstance().resolve(repository, null);
		if (repositoryRoot == null) {
			throw new IOException("The directory for the repository does not exist: " + repository);
		}
		
		String releaseFile = repositoryString.replaceAll("(.*/)[^/]+$", "$1release.xml");
		URI releaseUri = new URI(URIUtils.encodeURI(releaseFile));
		Resource releaseResource = ResourceFactory.getInstance().resolve(releaseUri, null);
		String imageName = null;
		String imageVersion = null;
		String imageEnvironment = null;
		Date imageDate = null;
		// if we have a release file, check it
		if (releaseResource instanceof ReadableResource) {
			ResourceReadableContainer resourceReadableContainer = new ResourceReadableContainer((ReadableResource) releaseResource);
			try {
				String releaseContent = new String(IOUtils.toBytes(resourceReadableContainer), "UTF-8");
				imageName = releaseContent.replaceAll("(?s).*<image>(.*?)</image>.*", "$1");
				imageVersion = releaseContent.replaceAll("(?s).*<version>(.*?)</version>.*", "$1");
				imageEnvironment = releaseContent.replaceAll("(?s).*<environment>(.*?)</environment>.*", "$1");
				String date = releaseContent.replaceAll("(?s).*<created>(.*?)</created>.*", "$1");
				// quick check to see we didn't get the whole xml
				if (!date.startsWith("<")) {
					SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
					formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
					try {
						imageDate = formatter.parse(date);
					}
					catch (Exception e) {
						logger.warn("Could not parse release date", e);
					}
				}
			}
			finally {
				resourceReadableContainer.close();
			}
		}
		else {
			logger.info("No release file found");
		}
		
		logger.debug("Starting license manager...");
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
			logger.debug("Getting deployments folder: "  + deploymentFolder);
			URI deployments = new URI(URIUtils.encodeURI(deploymentFolder));
			deploymentRoot = ResourceUtils.mkdir(deployments, null);
		}
		else {
			File file = new File("deployments");
			logger.debug("Getting deployments folder: "  + file);
			if (!file.exists()) {
				file.mkdir();
			}
			deploymentRoot = new FileDirectory(null, file, false);
		}
		
		RoleHandler roleHandler = null;
		if (getArgument("roles", null, args) != null) {
			logger.debug("Getting role handler...");
			roleHandler = (RoleHandler) Class.forName(getArgument("roles", null, args)).newInstance();	
		}

		logger.debug("Getting primary configuration...");
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
		
		if (groupName == null && imageName != null && imageEnvironment != null) {
			groupName = imageName + "-" + imageEnvironment;
		}
		
		if (serverName == null && groupName == null) {
			throw new IllegalArgumentException("Need to provide either the server name or the group name");
		}
		// in auto-clusters, we do provide the group name but not the server name as the instances are not configured ad priori
		// we generate a semi-unique yet semi-reusable server name
		if (serverName == null && groupName != null) {
			// the ip is semi-static and guaranteed unique within the relevant scope
			serverName = groupName + "-" + InetAddress.getLocalHost().getHostAddress();
		}
		
		logger.debug("Building repository...(snapshots: " + enableSnapshots + ")");
		// create the repository
		EAIResourceRepository repositoryInstance = new EAIResourceRepository(enableSnapshots ? SnapshotUtils.prepare(repositoryRoot) : repositoryRoot, mavenRoot);
		repositoryInstance.setHistorizeGauges(historizeGauges);
		repositoryInstance.enableMetrics(enableMetrics);
		repositoryInstance.setName(serverName);
		repositoryInstance.setGroup(groupName == null ? serverName : groupName);
		repositoryInstance.setLicenseManager(licenseManager);
		repositoryInstance.setHistorizationInterval(historizationInterval);
		repositoryInstance.setHistorySize(historySize);
		
		repositoryInstance.addEventEnricher("created", new CreatedDateEnricher());
		repositoryInstance.addEventEnricher("correlation-id", new CorrelationIdEnricher());
		repositoryInstance.addEventEnricher("authentication", new AuthenticationEnricher());
		
		if (aliasName != null) {
			logger.debug("Setting aliases: " + aliasName);
			repositoryInstance.getAliases().addAll(Arrays.asList(aliasName.split("[\\s]*,[\\s]*")));
		}

		if (roleService != null) {
			logger.debug("Setting role service...");
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
			logger.debug("Setting permission service...");
			Artifact resolve = repositoryInstance.resolve(permissionService);
			if (resolve == null) {
				logger.error("Invalid permission service: " + permissionService);
			}
			else {
				repositoryInstance.setPermissionHandler(POJOUtils.newProxy(PermissionHandler.class, repositoryInstance, SystemPrincipal.ROOT, (DefinedService) resolve));
			}
		}
		
		logger.debug("Building server...");
		server = new Server(roleHandler, repositoryInstance, startupEvent);
		
		// register this shutdown hook _before_ hazelcast, otherwise we can't correctly wind down hazelcast-based artifacts
		server.addShutdownHook();
		server.setImageEnvironment(imageEnvironment);
		server.setImageName(imageName);
		server.setImageVersion(imageVersion);
		server.setImageDate(imageDate);
		
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
			logger.debug("Configuring clustering...");
			
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
				// if we don't have an absolute url, we use the installation of the integrator as point of reference
				if (!cluster.startsWith("/") && !cluster.matches("^[\\w]+:/.*")) {
					cluster = getIntegratorPath() + "/" + cluster;
				}
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
			// we will shut it down ourselves to cleanly combine with our own shutdown routines
			config.setProperty("hazelcast.shutdownhook.enabled", "false");
			
			// hazelcast 3.12
			config.getMemberAttributeConfig().setStringAttribute("group", repositoryInstance.getGroup());
			config.getMemberAttributeConfig().setStringAttribute("name", repositoryInstance.getName());
			// hazelcast 4.2.4
//			config.getMemberAttributeConfig().setAttribute("group", repositoryInstance.getGroup());
//			config.getMemberAttributeConfig().setAttribute("name", repositoryInstance.getName());
			config.setClassLoader(repositoryInstance.getClassLoader());
			logger.debug("Creating cluster instance...");
	        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
			server.setCluster(new HazelcastClusterInstance(instance));
			server.addShutdownAction(new Runnable() {
				@Override
				public void run() {
					logger.info("Shutting down hazelcast");
					instance.shutdown();
				}
			});
		}
		else {
			server.setCluster(new LocalInstance());
		}
		
		if (logComplexEvents) {
			repositoryInstance.getComplexEventDispatcher().subscribe(Object.class, new CEFLogger(server));
		}
		logger.debug("Configuring event processors...");
		MultipleCEPProcessor cepProcessor = new MultipleCEPProcessor(server);
		server.setProcessor(cepProcessor);
		// if we have registered one...add it (partly for backwards compatibility)
		if (cepService != null) {
			for (String singleService : cepService.split(",")) {
				cepProcessor.add(singleService.trim());
			}
		}
		repositoryInstance.getComplexEventDispatcher().subscribe(Object.class, cepProcessor);
		
		MultipleMetricStatisticsProcessor metricProcessor = new MultipleMetricStatisticsProcessor(server);
		server.setMetricsStatisticsProcessor(metricProcessor);
		repositoryInstance.getMetricsDispatcher().subscribe(MetricStatistics.class, metricProcessor);
		
		logger.debug("Initializing server...");
		server.initialize();
		
		if (localMavenServer != null) {
			repositoryInstance.setLocalMavenServer(new URI(URIUtils.encodeURI(localMavenServer)));
			repositoryInstance.setUpdateMavenSnapshots(updateMavenSnapshots);
		}
		server.setAnonymousIsRoot(anonymousIsRoot);
		server.setListenerPoolSize(listenerPoolSize);
		server.setPort(port);
		logger.debug("Starting server...");
		server.start();

		String loggerService = getArgument("logger", null, args);
		String loggerAsync = getArgument("loggerAsync", null, args);
		if (loggerService != null) {
			server.enableLogger(loggerService, loggerAsync == null || !loggerAsync.equalsIgnoreCase("false"), getChildren("logger", args));
		}

		if (enableREST || enableMaven || enableRepository) {
			logger.debug("Finalizing server...");
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
		logger.info("------------------------------------ SERVER READY ------------------------------------");
	}

	public static String getIntegratorPath() {
		try {
			URL location = Standalone.class.getProtectionDomain().getCodeSource().getLocation();
			String asciiString = location.toURI().toASCIIString();
			// it's not entirely clear if you get the path to the library itself (which I got in my tests) or the path to the class _in_ the library (which seems indicated online)
			asciiString = asciiString.replaceAll("/lib/eai-server-[1-9.]+[\\w-]*\\.jar($|/.*)", "");
			return asciiString;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public Server getServer() {
		return server;
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
