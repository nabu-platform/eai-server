/*
* Copyright (C) 2015 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

package be.nabu.eai.server;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import be.nabu.libs.events.impl.EventDispatcherImpl;
import be.nabu.libs.http.api.HTTPRequest;
import be.nabu.libs.http.api.server.HTTPServer;
import be.nabu.libs.http.server.HTTPServerUtils;
import be.nabu.libs.maven.MavenListener;
import be.nabu.libs.maven.ResourceRepository;
import be.nabu.libs.maven.api.Repository;
import be.nabu.libs.resources.ResourceFactory;
import be.nabu.libs.resources.URIUtils;
import be.nabu.libs.resources.api.ResourceContainer;

public class MavenServer {
	public static void main(String...args) throws IOException, URISyntaxException {
		String filePath = args.length > 0 ? args[0] : "file:" + System.getProperty("user.home") + "/.m2/repository";
		System.out.println("Starting maven server on: " + filePath);
		
		Repository repository = new ResourceRepository((ResourceContainer<?>) ResourceFactory.getInstance().resolve(new URI(URIUtils.encodeURI(filePath)), null));
		
		HTTPServer server = HTTPServerUtils.newServer(8080, 20, new EventDispatcherImpl());
		server.getDispatcher(null).subscribe(HTTPRequest.class, new MavenListener(repository));
		server.start();
	}
}
