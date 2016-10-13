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
import be.nabu.libs.types.SimpleTypeWrapperFactory;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.base.SimpleElementImpl;
import be.nabu.libs.types.binding.api.Window;
import be.nabu.libs.types.binding.xml.XMLBinding;
import be.nabu.libs.types.structure.Structure;
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
		String repository = URIUtils.encodeURI(getSetting("repository"));
		URI uri = new URI(repository);
		// if we have no host, use the one from the endpoint
		if (uri.getScheme().equals("remote") && uri.getAuthority() == null) {
			uri = new URI(repository.replace("remote:", "remote://" + endpoint.getAuthority()));
		}
		System.out.println("REPOSITORY: " + uri);
		return uri;
	}
	
	public URI getMavenRoot() throws IOException, FormatException, ParseException, URISyntaxException {
		String maven = URIUtils.encodeURI(getSetting("maven"));
		URI uri = new URI(maven);
		// if we have no host, use the one from the endpoint
		if (uri.getScheme().equals("remote") && uri.getAuthority() == null) {
			uri = new URI(maven.replace("remote:", "remote://" + endpoint.getAuthority()));
		}
		System.out.println("MAVEN: " + uri);
		return uri;
	}
	
	public Boolean requiresAuthentication() throws UnsupportedEncodingException, IOException, FormatException, ParseException, URISyntaxException {
		String setting = getSetting("authentication");
		boolean result = setting == null ? false : Boolean.parseBoolean(setting);
		System.out.println("Server requires authentication: " + result);
		return result;
	}
	
	public String getName() throws UnsupportedEncodingException, IOException, FormatException, ParseException, URISyntaxException {
		return URIUtils.encodeURI(getSetting("name"));
	}
	
	public static boolean isOk(int code) {
		return code >= 200 && code < 300;
	}

	public void reload(String id) throws IOException, FormatException, ParseException {
		URI target = URIUtils.getChild(endpoint, "/reload/" + id);
		HTTPResponse response = request(HTTPUtils.get(target));
		if (!isOk(response.getCode())) {
			throw new IOException("The remote server sent back the code " + response.getCode() + ": " + response.getMessage());
		}
	}
	
	public void snapshot(String id) throws IOException, FormatException, ParseException {
		URI target = URIUtils.getChild(endpoint, "/snapshot/" + id);
		HTTPResponse response = request(HTTPUtils.get(target));
		if (!isOk(response.getCode())) {
			throw new IOException("The remote server sent back the code " + response.getCode() + ": " + response.getMessage());
		}
	}
	
	public void release(String id) throws IOException, FormatException, ParseException {
		URI target = URIUtils.getChild(endpoint, "/release/" + id);
		HTTPResponse response = request(HTTPUtils.get(target));
		if (!isOk(response.getCode())) {
			throw new IOException("The remote server sent back the code " + response.getCode() + ": " + response.getMessage());
		}
	}
	
	public void restore(String id) throws IOException, FormatException, ParseException {
		URI target = URIUtils.getChild(endpoint, "/restore/" + id);
		HTTPResponse response = request(HTTPUtils.get(target));
		if (!isOk(response.getCode())) {
			throw new IOException("The remote server sent back the code " + response.getCode() + ": " + response.getMessage());
		}
	}
	
	public void reloadAll() throws IOException, FormatException, ParseException {
		URI target = URIUtils.getChild(endpoint, "/reload");
		HTTPResponse response = request(HTTPUtils.get(target));
		if (!isOk(response.getCode())) {
			throw new IOException("The remote server sent back the code " + response.getCode() + ": " + response.getMessage());
		}
	}
	
	public void unload(String id) throws IOException, FormatException, ParseException {
		URI target = URIUtils.getChild(endpoint, "/unload/" + id);
		HTTPResponse response = request(HTTPUtils.get(target));
		if (!isOk(response.getCode())) {
			throw new IOException("The remote server sent back the code " + response.getCode() + ": " + response.getMessage());
		}
	}
	
	private String getSetting(String name) throws IOException, FormatException, ParseException, URISyntaxException, UnsupportedEncodingException {
		URI target = URIUtils.getChild(endpoint, "/settings/" + name);
		HTTPResponse response = request(HTTPUtils.get(target));
		if (!isOk(response.getCode())) {
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
	public Future<ServiceResult> run(Service service, ExecutionContext executionContext, ComplexContent input, ServiceRunnableObserver...arg3) {
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
			if (executionContext.getSecurityContext().getToken() != null) {
				request.getContent().setHeader(new MimeHeader("Run-As", executionContext.getSecurityContext().getToken().getName()));
			}
			HTTPResponse response = request(request);
			
			if (!isOk(response.getCode())) {
				throw new ServiceException("REMOTE-1", "Remote server code " + response.getCode() + ": " + response.getMessage(), response.getCode(), response.getMessage());
			}
			if (!(response.getContent() instanceof ContentPart)) {
				throw new ParseException("Expecting a content part as answer, received: " + response.getContent(), 0);
			}
			// it is possible that the result is simply empty (null) e.g. if the service is a java method with return type void
			if (!Long.valueOf(0).equals(MimeUtils.getContentLength(response.getContent().getHeaders()))) {
				// if the content type is "text/plain", an error was sent back as a stacktrace
				// TODO: could also send back an application/octet-stream with a serialized exception
				if ("text/plain".equals(MimeUtils.getContentType(response.getContent().getHeaders()))) {
					exception = new ServiceException("REMOTE-0", new String(IOUtils.toBytes((((ContentPart) response.getContent()).getReadable()))));
				}
				else {
					XMLBinding resultBinding = new XMLBinding(service.getServiceInterface().getOutputDefinition(), charset);
					resultBinding.setAllowSuperTypes(true);
					try {
						result = resultBinding.unmarshal(IOUtils.toInputStream(((ContentPart) response.getContent()).getReadable()), new Window[0]);
					}
					catch (Exception e) {
						// if we can't parse the content, show the original XML atm
						Structure structure = new Structure();
						structure.setName("response");
						structure.add(new SimpleElementImpl<String>("content", SimpleTypeWrapperFactory.getInstance().getWrapper().wrap(String.class), structure));
						result = structure.newInstance();
						result.set("content", new String(IOUtils.toBytes(((ContentPart) response.getContent()).getReadable()), "UTF-8"));
					}
				}
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

	public URI getEndpoint() {
		return endpoint;
	}

	public Principal getPrincipal() {
		return principal;
	}
	
}
