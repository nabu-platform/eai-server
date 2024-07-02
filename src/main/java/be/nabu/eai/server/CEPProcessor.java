package be.nabu.eai.server;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
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
import be.nabu.libs.services.api.ServiceException;
import be.nabu.libs.services.api.TransactionContext;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.utils.cep.api.ComplexEvent;
import be.nabu.utils.cep.api.EventSeverity;

public class CEPProcessor implements EventHandler<Object, Void> {

	private static final int POLL_INTERVAL = 5000;
	private static final int INTERRUPT_INTERVAL = 1000;
	private Server server;
	private String cepService;
	private DefinedService service;
	private Thread thread;
	private List<Object> events = new ArrayList<Object>();
	private Logger logger = LoggerFactory.getLogger(getClass());
	private EventSeverity cepSeverity = EventSeverity.valueOf(System.getProperty("cepSeverity", "INFO"));
	private boolean stopped;
	private boolean skipOnError = true;		// if the handler can't deal, it is generally better to toss the events (they are best effort anyway) rather than keep buffering them, as they are presumed to be high volume
	// we want to keep track of the last time we interrupted
	// if we are getting a lot of pushes and stay over 500, perhaps we can't process at all (e.g. websocket down)
	// if we keep interrupting the thread, we are wasting a lot of CPU cycles 
	private Date lastInterrupted;
	private EventSeverity prioritySeverity;
	
	public CEPProcessor(Server server, String cepService) {
		this.server = server;
		this.cepService = cepService;
	}
	
	public void stop() {
		stopped = true;
	}

	private boolean pushEvents(DefinedService service, List<Object> eventsToProcess) throws ServiceException {
		boolean handled = false;
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
		handled = true;
		ComplexContent run = serviceRuntime.run(newInstance);
		if (run != null && run.getType().get("handled") != null) {
			handled = run.get("handled") == null || (Boolean) run.get("handled");
		}
		return handled;
	}
	
	public void start() {
		Thread thread = new Thread(new Runnable() {
			@Override
			public void run() {
				while (!stopped) {
					try {
						if (events.size() > 0) {
							ArrayList<Object> eventsToProcess;
							DefinedService service = getService();
							synchronized(events) {
								eventsToProcess = new ArrayList<Object>(events);
							}
							if (service != null) {
								boolean handled = true;
								try {
									handled = pushEvents(service, eventsToProcess);
								}
								catch (Exception e) {
									if (!skipOnError) {
										throw e;
									}
								}
								finally {
									if (handled) {
										synchronized(events) {
											events.removeAll(eventsToProcess);
										}
									}
								}
							}
							else {
								logger.warn("Could not find event service: " + cepService);
								break;
							}
						}
					}
					catch (Exception e) {
						logger.error("Could not process complex events", e);
					}
					try {
						Thread.sleep(POLL_INTERVAL);
					}
					catch (InterruptedException e) {
						// ignore
					}
				}
				stopped = true;
			}
		});
		thread.setContextClassLoader(server.getRepository().getClassLoader());
		thread.setDaemon(true);
		thread.setName("cep-processor:" + cepService);
		thread.start();
		this.thread = thread;
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
		if (event != null && !stopped) {
			EventSeverity severity = null;
			// if we have an event that has a low severity, skip it
			if (event instanceof ComplexEvent) {
				severity = ((ComplexEvent) event).getSeverity();
			}
			else if (event instanceof ComplexContent && ((ComplexContent) event).getType().get("severity") != null) {
				Object object = ((ComplexContent) event).get("severity");
				if (object instanceof EventSeverity) {
					severity = (EventSeverity) object;
				}
			}
			if (severity != null && severity.ordinal() < cepSeverity.ordinal()) {
				return null;
			}
			if (severity != null && prioritySeverity != null && severity.ordinal() >= prioritySeverity.ordinal()) {
				try {
					pushEvents(getService(), Arrays.asList(event));
				}
				catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
			else {
				synchronized(events) {
					events.add(event);
				}
				// if it is getting too much, interrupt the thread for processing (if applicable)
				if (events.size() > 50 && thread != null) {
					Date interrupted = new Date();
					if (lastInterrupted == null || lastInterrupted.getTime() < interrupted.getTime() - INTERRUPT_INTERVAL) {
						lastInterrupted = interrupted;
						thread.interrupt();
					}
				}
				// we have too many and apparently we can't dump 'em
				if (events.size() > 500) {
					Object remove;
					synchronized(events) {	
						remove = events.remove(0);
					}
					logger.info("Not enough capacity for '" + cepService + "' to store event: " + remove);
				}
			}
		}
		return null;
	}

	public String getServiceId() {
		return cepService;
	}

	public boolean isStopped() {
		return stopped;
	}

	public EventSeverity getPrioritySeverity() {
		return prioritySeverity;
	}

	public void setPrioritySeverity(EventSeverity prioritySeverity) {
		this.prioritySeverity = prioritySeverity;
	}
	
}
