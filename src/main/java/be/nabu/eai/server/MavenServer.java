package be.nabu.eai.server;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import be.nabu.libs.events.impl.EventDispatcherImpl;
import be.nabu.libs.maven.MavenListener;
import be.nabu.libs.maven.ResourceRepository;
import be.nabu.libs.maven.api.Repository;
import be.nabu.libs.resources.ResourceFactory;
import be.nabu.libs.resources.URIUtils;
import be.nabu.libs.resources.api.ResourceContainer;
import be.nabu.utils.http.api.HTTPRequest;
import be.nabu.utils.http.api.server.HTTPServer;
import be.nabu.utils.http.server.DefaultHTTPServer;

public class MavenServer {
	public static void main(String...args) throws IOException, URISyntaxException {
		String filePath = args.length > 0 ? args[0] : "file:" + System.getProperty("user.home") + "/.m2/repository";
		System.out.println("Starting maven server on: " + filePath);
		
		Repository repository = new ResourceRepository((ResourceContainer<?>) ResourceFactory.getInstance().resolve(new URI(URIUtils.encodeURI(filePath)), null));
		
		HTTPServer server = new DefaultHTTPServer(8080, 20, new EventDispatcherImpl());
		server.getEventDispatcher().subscribe(HTTPRequest.class, new MavenListener(repository));
		server.start();
	}
}
