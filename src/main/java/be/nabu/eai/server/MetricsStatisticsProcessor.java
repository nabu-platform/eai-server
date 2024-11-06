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

import java.util.ArrayList;
import java.util.Date;
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

	private static final int POLL_INTERVAL = 5000;
	private static final int INTERRUPT_INTERVAL = 1000;
	private Server server;
	private String metricService;
	private DefinedService service;
	private Thread thread;
	private List<MetricStatistics> metrics = new ArrayList<MetricStatistics>();
	private Logger logger = LoggerFactory.getLogger(getClass());
	private boolean stopped;
	private boolean skipOnError = true;		// if the handler can't deal, it is generally better to toss the metrics (they are best effort anyway) rather than keep buffering them, as they are presumed to be high volume
	// we want to keep track of the last time we interrupted
	// if we are getting a lot of pushes and stay over 500, perhaps we can't process at all (e.g. websocket down)
	// if we keep interrupting the thread, we are wasting a lot of CPU cycles 
	private Date lastInterrupted;

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
								finally {
									if (handled) {
										synchronized(metrics) {
											metrics.removeAll(metricsToProcess);
										}
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
						logger.error("Could not process metrics", e);
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
				Date interrupted = new Date();
				if (lastInterrupted == null || lastInterrupted.getTime() < interrupted.getTime() - INTERRUPT_INTERVAL) {
					lastInterrupted = interrupted;
					thread.interrupt();
				}
			}
			// we have too many and apparently we can't dump 'em
			if (metrics.size() > 500) {
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
