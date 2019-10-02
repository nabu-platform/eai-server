package be.nabu.eai.server.api;

import java.util.List;

import javax.jws.WebParam;
import javax.jws.WebResult;

public interface EventHandler {
	@WebResult(name = "handled")
	public boolean handle(@WebParam(name = "events") List<Object> events);
}
