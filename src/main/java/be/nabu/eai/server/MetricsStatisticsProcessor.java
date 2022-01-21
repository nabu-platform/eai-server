package be.nabu.eai.server;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.eai.repository.util.MetricStatistics;
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

public class MetricsStatisticsProcessor implements EventHandler<MetricStatistics, Void> {

	private Server server;
	private String metricService;
	private DefinedService service;
	private Thread thread;
	private List<MetricStatistics> metrics = new ArrayList<MetricStatistics>();
	private Logger logger = LoggerFactory.getLogger(getClass());
	private boolean stopped;
	private boolean skipOnError = true;		// if the handler can't deal, it is generally better to toss the events (they are best effort anyway) rather than keep buffering them, as they are presumed to be high volume

	public MetricsStatisticsProcessor(Server server, String metricService) {
		this.server = server;
		this.metricService = metricService;
	}
	
	public void stop() {
		stopped = true;
	}

	public void start() {
		Thread thread = new Thread(new Runnable() {
			@Override
			public void run() {
				while (!stopped) {
					try {
						if (metrics.size() > 0) {
							ArrayList<MetricStatistics> metricsToProcess;
							DefinedService service = getService();
							synchronized(metrics) {
								metricsToProcess = new ArrayList<MetricStatistics>(metrics);
							}
							if (service != null) {
								ComplexContent newInstance = service.getServiceInterface().getInputDefinition().newInstance();
								newInstance.set("metrics", metricsToProcess);
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

									// no eventing, we don't want to trace these calls
									@Override
									public EventTarget getEventTarget() {
										return null;
									}
									
								});
								boolean handled = true;
								try {
									ComplexContent run = serviceRuntime.run(newInstance);
									if (run != null && run.getType().get("handled") != null) {
										handled = run.get("handled") == null || (Boolean) run.get("handled");
									}
								}
								catch (Exception e) {
									if (!skipOnError) {
										throw e;
									}
								}
								if (handled) {
									synchronized(metrics) {
										metrics.removeAll(metricsToProcess);
									}
								}
							}
							else {
								logger.warn("Could not find event service: " + metricService);
								break;
							}
						}
					}
					catch (Exception e) {
						logger.error("Could not process complex events", e);
					}
					try {
						Thread.sleep(5000);
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
		thread.setName("metric-statistics-processor:" + metricService);
		thread.start();
		this.thread = thread;
	}

	private DefinedService getService() {
		if (service == null) {
			synchronized(this) {
				if (service == null) {
					service = (DefinedService) server.getRepository().resolve(metricService);
				}
			}
		}
		return service;
	}
	
	@Override
	public Void handle(MetricStatistics event) {
		if (event != null && !stopped) {
			synchronized(metrics) {
				metrics.add(event);
			}
			// if it is getting too much, interrupt the thread for processing (if applicable)
			if (metrics.size() > 50 && thread != null) {
				thread.interrupt();
			}
			// we have too many and apparently we can't dump 'em
			else if (metrics.size() > 500) {
				Object remove;
				synchronized(metrics) {	
					remove = metrics.remove(0);
				}
				logger.info("Not enough capacity for '" + metricService + "' to store metric: " + remove);
			}
		}
		return null;
	}

	public String getServiceId() {
		return metricService;
	}

	public boolean isStopped() {
		return stopped;
	}
	
}
