package be.nabu.eai.server.rest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;

import be.nabu.eai.repository.api.Repository;
import be.nabu.eai.server.Server;
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

public class ServerREST {
	
	@Context
	private Repository repository;
	
	@Context
	private Server server;
	
	private SecurityContext securityContext;
	
	@Path("/invoke/{service}")
	@POST
	public Part invoke(@PathParam("service") String serviceId, InputStream content, Header...headers) throws IOException, ParseException, ServiceException {
		DefinedService service = (DefinedService) repository.getNode(serviceId);
		if (service == null) {
			throw new IllegalArgumentException("Can not find a service with the id: " + serviceId);
		}
		
		String contentType = MimeUtils.getContentType(headers);
		UnmarshallableBinding binding = MediaType.APPLICATION_JSON.equals(contentType) 
			? new JSONBinding(service.getServiceInterface().getInputDefinition())
			: new XMLBinding(service.getServiceInterface().getInputDefinition(), Charset.forName("UTF-8"));
		
		ComplexContent input = binding.unmarshal(content, new Window[0]);
		
		// need to return the output & the id of the thread it is running in
		Future<ServiceResult> future = repository.getServiceRunner().run(service, repository.newExecutionContext(securityContext == null ? null : securityContext.getUserPrincipal()), input);
		try {
			ServiceResult serviceResult = future.get();
			if (serviceResult.getException() != null) {
				throw serviceResult.getException();
			}
			ComplexContent output = serviceResult.getOutput();
			MarshallableBinding marshallable = MediaType.APPLICATION_JSON.equals(contentType) 
				? new JSONBinding(output.getType())
				: new XMLBinding(output.getType(), Charset.forName("UTF-8"));
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			marshallable.marshal(bytes, output);
			byte[] byteArray = bytes.toByteArray();
			return new PlainMimeContentPart(null, IOUtils.wrap(byteArray, true), 
				new MimeHeader("Content-Length", Integer.valueOf(byteArray.length).toString()),
				new MimeHeader("Content-Type", marshallable instanceof JSONBinding ? MediaType.APPLICATION_JSON : MediaType.APPLICATION_XML)
			);
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
	public URI getRepository() {
		return server.getRepositoryRoot();
	}
}
