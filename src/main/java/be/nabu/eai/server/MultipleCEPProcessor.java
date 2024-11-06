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

import be.nabu.libs.events.api.EventHandler;
import be.nabu.utils.cep.api.EventSeverity;

public class MultipleCEPProcessor implements EventHandler<Object, Void> {

	private List<CEPProcessor> processors = new ArrayList<CEPProcessor>();
	private Server server;
	private boolean started;
	
	public MultipleCEPProcessor(Server server) {
		this.server = server;
	}
	
	@Override
	public Void handle(Object event) {
		Iterator<CEPProcessor> iterator = processors.iterator();
		while (iterator.hasNext()) {
			CEPProcessor processor = iterator.next();
			try {
				processor.handle(event);
				if (processor.isStopped()) {
					iterator.remove();
				}
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	public void add(String serviceId) {
		add(serviceId, EventSeverity.ALERT);
	}
	
	public void add(String serviceId, EventSeverity prioritySeverity) {
		// remove any other instance of this service
		remove(serviceId);
		CEPProcessor cepProcessor = new CEPProcessor(server, serviceId);
		cepProcessor.setPrioritySeverity(prioritySeverity);
		add(cepProcessor);
		// if already started, immediately go at it!
		if (started) {
			cepProcessor.start();
		}
	}
	
	public void add(CEPProcessor processor) {
		// structured like this to avoid concurrency issues with the handle
		List<CEPProcessor> newProcessors = new ArrayList<CEPProcessor>(processors);
		newProcessors.add(processor);
		processors = newProcessors;
	}
	
	public void remove(String serviceId) {
		for (CEPProcessor processor : new ArrayList<CEPProcessor>(processors)) {
			if (processor.getServiceId().equals(serviceId)) {
				processor.stop();
			}
		}
	}
	
	public void start() {
		for (CEPProcessor processor : new ArrayList<CEPProcessor>(processors)) {
			try {
				processor.start();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		started = true;
	}

	public List<CEPProcessor> getProcessors() {
		return processors;
	}
	
}
