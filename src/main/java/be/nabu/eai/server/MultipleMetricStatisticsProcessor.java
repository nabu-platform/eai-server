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
import java.util.Iterator;
import java.util.List;

import be.nabu.eai.repository.util.MetricStatistics;
import be.nabu.libs.events.api.EventHandler;

public class MultipleMetricStatisticsProcessor implements EventHandler<MetricStatistics, Void> {

	private List<MetricsStatisticsProcessor> processors = new ArrayList<MetricsStatisticsProcessor>();
	private Server server;
	private boolean started;

	public MultipleMetricStatisticsProcessor(Server server) {
		this.server = server;
	}

	@Override
	public Void handle(MetricStatistics event) {
		Iterator<MetricsStatisticsProcessor> iterator = processors.iterator();
		while (iterator.hasNext()) {
			MetricsStatisticsProcessor processor = iterator.next();
			try {
				processor.handle(event);
				if (processor.isStopped()) {
					iterator.remove();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	public void add(String serviceId) {
		// remove any other instance of this service
		remove(serviceId);
		MetricsStatisticsProcessor metricProcessor = new MetricsStatisticsProcessor(server, serviceId);
		add(metricProcessor);
		// if already started, immediately go at it!
		if (started) {
			metricProcessor.start();
		}
	}

	public void add(MetricsStatisticsProcessor processor) {
		// structured like this to avoid concurrency issues with the handle
		List<MetricsStatisticsProcessor> newProcessors = new ArrayList<MetricsStatisticsProcessor>(processors);
		newProcessors.add(processor);
		processors = newProcessors;
	}

	public void remove(String serviceId) {
		for (MetricsStatisticsProcessor processor : new ArrayList<MetricsStatisticsProcessor>(processors)) {
			if (processor.getServiceId().equals(serviceId)) {
				processor.stop();
			}
		}
	}

	public void start() {
		for (MetricsStatisticsProcessor processor : new ArrayList<MetricsStatisticsProcessor>(processors)) {
			try {
				processor.start();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		started = true;
	}

	public List<MetricsStatisticsProcessor> getProcessors() {
		return processors;
	}

}
