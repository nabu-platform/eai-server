package be.nabu.eai.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.io.BufferedInputStream;

import be.nabu.libs.events.impl.EventDispatcherImpl;
import be.nabu.libs.resources.ResourceFactory;
import be.nabu.libs.resources.URIUtils;
import be.nabu.libs.resources.api.ResourceContainer;
import be.nabu.utils.http.api.server.HTTPServer;
import be.nabu.utils.http.rest.RoleHandler;
import be.nabu.utils.http.server.DefaultHTTPServer;

public class Standalone {
	
	public static void main(String...args) throws IOException, URISyntaxException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		File propertiesFile = new File("server.properties");
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
		
		int port = new Integer(getArgument("port", "5001", args));
		int listenerPoolSize = new Integer(getArgument("listenerPoolSize", "20", args));

		URI repository = new URI(URIUtils.encodeURI(getMandatoryArgument("repository", args)));
		ResourceContainer<?> repositoryRoot = (ResourceContainer<?>) ResourceFactory.getInstance().resolve(repository, null);
		if (repositoryRoot == null) {
			throw new IOException("The directory for the repository does not exist: " + repository);
		}
		
		RoleHandler roleHandler = null;
		if (getArgument("roles", null, args) != null) {
			roleHandler = (RoleHandler) Class.forName(getArgument("roles", null, args)).newInstance();	
		}

		HTTPServer http = new DefaultHTTPServer(port, listenerPoolSize, new EventDispatcherImpl());
		Server server = new Server(http, roleHandler, repositoryRoot);
		server.start();
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
