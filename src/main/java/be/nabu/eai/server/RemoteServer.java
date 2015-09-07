package be.nabu.eai.server;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.security.Principal;
import java.text.ParseException;
import java.util.concurrent.Future;

import be.nabu.libs.http.api.HTTPRequest;
import be.nabu.libs.http.api.HTTPResponse;
import be.nabu.libs.http.api.client.ClientAuthenticationHandler;
import be.nabu.libs.http.api.client.HTTPClient;
import be.nabu.libs.http.core.DefaultHTTPRequest;
import be.nabu.libs.http.core.HTTPUtils;
import be.nabu.libs.resources.URIUtils;
import be.nabu.libs.services.SimpleServiceResult;
import be.nabu.libs.services.api.DefinedService;
import be.nabu.libs.services.api.ExecutionContext;
import be.nabu.libs.services.api.Service;
import be.nabu.libs.services.api.ServiceException;
import be.nabu.libs.services.api.ServiceResult;
import be.nabu.libs.services.api.ServiceRunnableObserver;
import be.nabu.libs.services.api.ServiceRunner;
import be.nabu.libs.services.api.ServiceRuntimeTracker;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.binding.api.Window;
import be.nabu.libs.types.binding.xml.XMLBinding;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.mime.api.ContentPart;
import be.nabu.utils.mime.impl.FormatException;
import be.nabu.utils.mime.impl.MimeHeader;
import be.nabu.utils.mime.impl.MimeUtils;
import be.nabu.utils.mime.impl.PlainMimeContentPart;

public class RemoteServer implements ServiceRunner {
	
	private Charset charset;
	private HTTPClient client;
	private URI endpoint;
	private Principal principal;
	private ClientAuthenticationHandler authenticationHandler;

	public RemoteServer(HTTPClient client, URI endpoint, Principal principal, Charset charset) {
		this.client = client;
		this.endpoint = endpoint;
		this.principal = principal;
		this.charset = charset;
	}
	
	public URI getRepositoryRoot() throws IOException, FormatException, ParseException, URISyntaxException {
		return new URI(URIUtils.encodeURI(getSetting("repository")));
	}
	
	public URI getMavenRoot() throws IOException, FormatException, ParseException, URISyntaxException {
		return new URI(URIUtils.encodeURI(getSetting("maven")));
	}

	public void reload(String id) throws IOException, FormatException, ParseException {
		URI target = URIUtils.getChild(endpoint, "/reload/" + id);
		HTTPResponse response = request(HTTPUtils.get(target));
		if (response.getCode() != 200) {
			throw new IOException("The remote server sent back the code " + response.getCode() + ": " + response.getMessage());
		}
	}
	
	public void unload(String id) throws IOException, FormatException, ParseException {
		URI target = URIUtils.getChild(endpoint, "/unload/" + id);
		HTTPResponse response = request(HTTPUtils.get(target));
		if (response.getCode() != 200) {
			throw new IOException("The remote server sent back the code " + response.getCode() + ": " + response.getMessage());
		}
	}
	
	private String getSetting(String name) throws IOException, FormatException, ParseException, URISyntaxException, UnsupportedEncodingException {
		URI target = URIUtils.getChild(endpoint, "/settings/" + name);
		HTTPResponse response = request(HTTPUtils.get(target));
		if (response.getCode() != 200) {
			throw new IOException("The remote server sent back the code " + response.getCode() + ": " + response.getMessage());
		}
		if (!(response.getContent() instanceof ContentPart)) {
			throw new ParseException("Expecting a content part as answer, received: " + response.getContent(), 0);
		}
		return new String(IOUtils.toBytes(((ContentPart) response.getContent()).getReadable()), "UTF-8");
	}
	
	private HTTPResponse request(HTTPRequest request) throws IOException, FormatException, ParseException {
		// if we can, force authentication because we know the server will require it anyway, this saves us a trip
		// we also know the server requires basic authentication
		if (authenticationHandler != null) {
			String authenticationResponse = authenticationHandler.authenticate(principal, "basic");
			if (authenticationResponse != null) {
				request.getContent().setHeader(new MimeHeader(HTTPUtils.SERVER_AUTHENTICATE_RESPONSE, authenticationResponse));
			}
		}
		return client.execute(request, principal, endpoint.getScheme().equalsIgnoreCase("https"), true);
	}

	@Override
	public Future<ServiceResult> run(Service service, ExecutionContext arg1, ComplexContent input, ServiceRuntimeTracker serviceRuntimeTracker, ServiceRunnableObserver... arg3) {
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
			HTTPResponse response = request(request);
			
			if (response.getCode() != 200) {
				throw new ServiceException("Remote server code " + response.getCode() + ": " + response.getMessage());
			}
			if (!(response.getContent() instanceof ContentPart)) {
				throw new ParseException("Expecting a content part as answer, received: " + response.getContent(), 0);
			}
			// it is possible that the result is simply empty (null) e.g. if the service is a java method with return type void
			if (!Long.valueOf(0).equals(MimeUtils.getContentLength(response.getContent().getHeaders()))) {
				XMLBinding resultBinding = new XMLBinding(service.getServiceInterface().getOutputDefinition(), charset);
				result = resultBinding.unmarshal(IOUtils.toInputStream(((ContentPart) response.getContent()).getReadable()), new Window[0]);
			}
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
