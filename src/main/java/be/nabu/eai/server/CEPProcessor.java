package be.nabu.eai.server;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.eai.repository.util.SystemPrincipal;
import be.nabu.libs.events.api.EventHandler;
import be.nabu.libs.events.api.EventTarget;
import be.nabu.libs.metrics.api.MetricInstance;
import be.nabu.libs.services.ServiceRuntime;
import be.nabu.libs.services.api.DefinedService;
import be.nabu.libs.services.api.ExecutionContext;
import be.nabu.libs.services.api.SecurityContext;
import be.nabu.libs.services.api.ServiceContext;
import be.nabu.libs.services.api.TransactionContext;
import be.nabu.libs.types.api.ComplexContent;

public class CEPProcessor implements EventHandler<Object, Void> {

	private Server server;
	private String cepService;
	private DefinedService service;
	private Thread thread;
	private List<Object> events = new ArrayList<Object>();
	private Logger logger = LoggerFactory.getLogger(getClass());

	public CEPProcessor(Server server, String cepService) {
		this.server = server;
		this.cepService = cepService;
		this.thread = new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					try {
						if (events.size() > 0) {
							ArrayList<Object> eventsToProcess;
							synchronized(events) {
								eventsToProcess = new ArrayList<Object>(events);
								events.removeAll(eventsToProcess);
							}
							DefinedService service = getService();
							if (service != null) {
								ComplexContent newInstance = service.getServiceInterface().getInputDefinition().newInstance();
								newInstance.set("events", eventsToProcess);
								ExecutionContext ec = server.getRepository().newExecutionContext(SystemPrincipal.ROOT);
								ServiceRuntime serviceRuntime = new ServiceRuntime(service, new ExecutionContext() {
									@Override
									public MetricInstance getMetricInstance(String id) {
										return ec.getMetricInstance(id);
									}

									@Override
									public ServiceContext getServiceContext() {
										return ec.getServiceContext();
									}

									@Override
									public TransactionContext getTransactionContext() {
										return ec.getTransactionContext();
									}

									@Override
									public SecurityContext getSecurityContext() {
										return ec.getSecurityContext();
									}

									@Override
									public boolean isDebug() {
										return false;
									}

									// no eventing, we don't want to trace these calls!!
									// otherwise we might end up in unending loops
									@Override
									public EventTarget getEventTarget() {
										return null;
									}
									
								});
								serviceRuntime.run(newInstance);
							}
							else {
								logger.warn("Could not find event service: " + cepService);
							}
						}
					}
					catch (Exception e) {
						logger.error("Could not process complex events", e);
					}
					try {
						Thread.sleep(10000);
					}
					catch (InterruptedException e) {
						// ignore
					}
				}
			}
		});
		this.thread.start();
	}

	private DefinedService getService() {
		if (service == null) {
			synchronized(this) {
				if (service == null) {
					service = (DefinedService) server.getRepository().resolve(cepService);
				}
			}
		}
		return service;
	}
	
	@Override
	public Void handle(Object event) {
		if (event != null) {
			synchronized(events) {
				events.add(event);
			}
			// if it is getting too much, interrupt the thread for processing (if applicable)
			if (events.size() > 50) {
				thread.interrupt();
			}
		}
		return null;
	}

}
