package be.nabu.eai.server;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.security.Principal;
import java.text.ParseException;
import java.util.concurrent.Future;

import be.nabu.libs.resources.URIUtils;
import be.nabu.libs.services.SimpleServiceResult;
import be.nabu.libs.services.api.DefinedService;
import be.nabu.libs.services.api.ExecutionContext;
import be.nabu.libs.services.api.Service;
import be.nabu.libs.services.api.ServiceException;
import be.nabu.libs.services.api.ServiceResult;
import be.nabu.libs.services.api.ServiceRunnableObserver;
import be.nabu.libs.services.api.ServiceRunner;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.binding.api.Window;
import be.nabu.libs.types.binding.xml.XMLBinding;
import be.nabu.utils.http.DefaultHTTPRequest;
import be.nabu.utils.http.HTTPUtils;
import be.nabu.utils.http.api.AuthenticationHandler;
import be.nabu.utils.http.api.HTTPResponse;
import be.nabu.utils.http.api.client.HTTPClient;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.mime.api.ContentPart;
import be.nabu.utils.mime.impl.FormatException;
import be.nabu.utils.mime.impl.MimeHeader;
import be.nabu.utils.mime.impl.PlainMimeContentPart;
import be.nabu.utils.mime.impl.PlainMimeEmptyPart;

public class RemoteServer implements ServiceRunner {
	
	private Charset charset;
	private HTTPClient client;
	private URI endpoint;
	private Principal principal;
	private AuthenticationHandler authenticationHandler;

	public RemoteServer(HTTPClient client, URI endpoint, Principal principal, Charset charset) {
		this.client = client;
		this.endpoint = endpoint;
		this.principal = principal;
		this.charset = charset;
	}
	
	public URI getRepositoryRoot() throws IOException, FormatException, ParseException, URISyntaxException {
		URI target = URIUtils.getChild(endpoint, "/settings/repository");
		DefaultHTTPRequest request = new DefaultHTTPRequest(
			"GET",
			target.getPath(),
			new PlainMimeEmptyPart(null,
				new MimeHeader("Content-Length", "0"),
				new MimeHeader("Host", endpoint.getAuthority())
			)
		);
		// if we can, force authentication because we know the server will require it anyway, this saves us a trip
		// we also know the server requires basic authentication
		if (authenticationHandler != null) {
			String authenticationResponse = authenticationHandler.authenticate(principal, "basic");
			if (authenticationResponse != null) {
				request.getContent().setHeader(new MimeHeader(HTTPUtils.SERVER_AUTHENTICATE_RESPONSE, authenticationResponse));
			}
		}
		HTTPResponse response = client.execute(request, principal, endpoint.getScheme().equalsIgnoreCase("https"), true);
		if (response.getCode() != 200) {
			throw new IOException("The remote server sent back the code " + response.getCode() + ": " + response.getMessage());
		}
		if (!(response.getContent() instanceof ContentPart)) {
			throw new ParseException("Expecting a content part as answer, received: " + response.getContent(), 0);
		}
		return new URI(URIUtils.encodeURI(new String(IOUtils.toBytes(((ContentPart) response.getContent()).getReadable()), "UTF-8"))); 
	}

	@Override
	public Future<ServiceResult> run(Service service, ExecutionContext arg1, ComplexContent input, ServiceRunnableObserver... arg3) {
		if (!(service instanceof DefinedService)) {
			throw new IllegalArgumentException("The service has to be a defined one for remote execution");
		}
		URI target = URIUtils.getChild(endpoint, "/invoke/" + ((DefinedService) service).getId());
		XMLBinding xmlBinding = new XMLBinding(input.getType(), charset);
		ServiceException exception = null;
		ComplexContent result = null;
		try {
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			xmlBinding.marshal(output, input);
			byte [] content = output.toByteArray();
			DefaultHTTPRequest request = new DefaultHTTPRequest(
				"POST",
				target.getPath(),
				new PlainMimeContentPart(null, IOUtils.wrap(content, true),
					new MimeHeader("Content-Length", "" + content.length),
					new MimeHeader("Content-Type", "application/xml"),
					new MimeHeader("Host", endpoint.getAuthority())
				)
			);
			// if we can, force authentication because we know the server will require it anyway, this saves us a trip
			// we also know the server requires basic authentication
			if (authenticationHandler != null) {
				String authenticationResponse = authenticationHandler.authenticate(principal, "basic");
				if (authenticationResponse != null) {
					request.getContent().setHeader(new MimeHeader(HTTPUtils.SERVER_AUTHENTICATE_RESPONSE, authenticationResponse));
				}
			}
			HTTPResponse response = client.execute(request, principal, endpoint.getScheme().equalsIgnoreCase("https"), true);
			
			if (response.getCode() != 200) {
				throw new ServiceException("The remote server sent back the code " + response.getCode() + ": " + response.getMessage());
			}
			if (!(response.getContent() instanceof ContentPart)) {
				throw new ParseException("Expecting a content part as answer, received: " + response.getContent(), 0);
			}
			
			XMLBinding resultBinding = new XMLBinding(service.getServiceInterface().getOutputDefinition(), charset);
			result = resultBinding.unmarshal(IOUtils.toInputStream(((ContentPart) response.getContent()).getReadable()), new Window[0]);
		}
		catch (FormatException e) {
			exception = new ServiceException(e);
		}
		catch (ParseException e) {
			exception = new ServiceException(e);
		}
		catch (IOException e) {
			exception = new ServiceException(e);
		}
		catch (ServiceException e) {
			exception = e;
		}
		return new Server.ServiceResultFuture(new SimpleServiceResult(result, exception));
	}
	
}
