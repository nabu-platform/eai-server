package be.nabu.eai.server.rest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.eai.repository.api.Repository;
import be.nabu.eai.repository.util.SystemPrincipal;
import be.nabu.eai.server.Server;
import be.nabu.libs.authentication.api.Token;
import be.nabu.libs.http.core.ServerHeader;
import be.nabu.libs.services.api.DefinedService;
import be.nabu.libs.services.api.ServiceException;
import be.nabu.libs.services.api.ServiceResult;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.binding.api.MarshallableBinding;
import be.nabu.libs.types.binding.api.UnmarshallableBinding;
import be.nabu.libs.types.binding.api.Window;
import be.nabu.libs.types.binding.json.JSONBinding;
import be.nabu.libs.types.binding.xml.XMLBinding;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.mime.api.Header;
import be.nabu.utils.mime.api.Part;
import be.nabu.utils.mime.impl.MimeHeader;
import be.nabu.utils.mime.impl.MimeUtils;
import be.nabu.utils.mime.impl.PlainMimeContentPart;
import be.nabu.utils.mime.impl.PlainMimeEmptyPart;

public class ServerREST {
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	@Context
	private Repository repository;
	
	@Context
	private Server server;
	
	@Context
	private SecurityContext securityContext;

	@Path("/reload")
	@GET
	public void reloadAll() {
		repository.reloadAll();
	}

	@Path("/reload/{id}")
	@GET
	public void reload(@PathParam("id") String id) {
		repository.reload(id);
	}
	
	@Path("/unload/{id}")
	@GET
	public void unload(@PathParam("id") String id) {
		repository.unload(id);
	}
	
	@Path("/invoke/{service}")
	@POST
	public Part invoke(@PathParam("service") String serviceId, InputStream content, Header...headers) throws IOException, ParseException, ServiceException {
		logger.debug("Invoking: {}", serviceId);
		DefinedService service = (DefinedService) repository.resolve(serviceId);
		if (service == null) {
			throw new IllegalArgumentException("Can not find the service with id: " + serviceId);
		}
		String contentType = MimeUtils.getContentType(headers);

		UnmarshallableBinding binding = MediaType.APPLICATION_JSON.equals(contentType) 
			? new JSONBinding(service.getServiceInterface().getInputDefinition())
			: new XMLBinding(service.getServiceInterface().getInputDefinition(), Charset.forName("UTF-8"));
		
		ComplexContent input = binding.unmarshal(content, new Window[0]);
		
		// need to return the output & the id of the thread it is running in
		Token principal = securityContext == null ? null : (Token) securityContext.getUserPrincipal();
		if (principal == null && server.isAnonymousIsRoot()) {
			principal = SystemPrincipal.ROOT;
		}
		
		final Header header = MimeUtils.getHeader("Run-As", headers);
		if (header != null) {
			principal = new SystemPrincipal(header.getValue());
		}
		
		Future<ServiceResult> future = repository.getServiceRunner().run(service, repository.newExecutionContext(principal), input);
		try {
			ServiceResult serviceResult = future.get();
			if (serviceResult.getException() != null) {
				logger.error("Could not run service: " + serviceId, serviceResult.getException());
				StringWriter writer = new StringWriter();
				PrintWriter printer = new PrintWriter(writer);
				serviceResult.getException().printStackTrace(printer);
				printer.flush();
				byte [] bytes = writer.toString().getBytes();
				return new PlainMimeContentPart(null, IOUtils.wrap(bytes, true),
					new MimeHeader("Content-Length", Integer.valueOf(bytes.length).toString()),
					new MimeHeader("Content-Type", "text/plain")
				);
			}
			ComplexContent output = serviceResult.getOutput();
			// this is possible in some cases (e.g. void java methods)
			if (output == null) {
				return new PlainMimeEmptyPart(null, 
					new MimeHeader("Content-Length", "0")
				);
			}
			else {
				MarshallableBinding marshallable = MediaType.APPLICATION_JSON.equals(contentType) 
					? new JSONBinding(output.getType())
					: new XMLBinding(output.getType(), Charset.forName("UTF-8"));
				ByteArrayOutputStream bytes = new ByteArrayOutputStream();
				marshallable.marshal(bytes, output);
				byte[] byteArray = bytes.toByteArray();
				logger.trace("Response: {}", new String(byteArray));
				return new PlainMimeContentPart(null, IOUtils.wrap(byteArray, true), 
					new MimeHeader("Content-Length", Integer.valueOf(byteArray.length).toString()),
					new MimeHeader("Content-Type", marshallable instanceof JSONBinding ? MediaType.APPLICATION_JSON : MediaType.APPLICATION_XML)
				);
			}
		}
		catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		catch (ExecutionException e) {
			throw new RuntimeException(e);
		}
	}
	
	@GET
	@Path("/settings/repository")
	public URI getRepository(@HeaderParam(value = ServerHeader.NAME_REMOTE_IS_LOCAL) String isLocal) throws URISyntaxException {
		// we send back the "original" repository the server connected to if:
		// - we did not enable repository sharing on this server
		// - the connection is from the same host as the server is running on (usually development mode)
		// note that we don't send the server authority in the remote URL, the client is supposed to resolve a missing authority as the server itself (whatever that is)
		// also note that in the beginning we would send back the repository itself IF it was already remote
		// however it is entirely possible that the developer does have access to the server but not the repository (firewall-wise)
		// if you want to hook up a server to a remote repository and have it report that to the developer, disable repository sharing
		return server.isEnabledRepositorySharing() && (!"true".equals(isLocal) || server.isForceRemoteRepository()) ? new URI("remote:/repository") : server.getRepositoryRoot();
	}
	
	@GET
	@Path("/settings/maven")
	public URI getMaven(@HeaderParam(value = ServerHeader.NAME_REMOTE_IS_LOCAL) String isLocal) throws URISyntaxException {
		return server.isEnabledRepositorySharing() && (!"true".equals(isLocal) || server.isForceRemoteRepository()) ? new URI("remote:/modules") : server.getRepository().getMavenRoot();
	}
	
	@GET
	@Path("/settings/name")
	public String getName() {
		return server.getName();
	}
	
}
