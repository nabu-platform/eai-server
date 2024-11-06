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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import be.nabu.libs.services.api.ServiceResult;

public class BatchResultFuture implements Future<List<ServiceResult>> {
	
	private List<ServiceResult> results = new ArrayList<ServiceResult>();
	private int amountOfExpectedResults;
	private boolean cancelled;
	
	private CountDownLatch latch = null;

	public BatchResultFuture(int amountOfExpectedResults) {
		latch = new CountDownLatch(amountOfExpectedResults);
	}
	
	public void addResult(ServiceResult result) {
		synchronized(results) {
			results.add(result);
			latch.countDown();
		}
	}
	
	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		boolean cancelledSomething = false;
		// lower our expectations...
		while (latch.getCount() > 0) {
			latch.countDown();
			cancelledSomething = true;
		}
		// can't cancel if it was already done for example
		if (cancelledSomething) {
			cancelled = true;
		}
		return cancelledSomething;
	}

	@Override
	public boolean isCancelled() {
		return cancelled;
	}

	@Override
	public boolean isDone() {
		return results.size() >= amountOfExpectedResults;
	}

	@Override
	public List<ServiceResult> get() throws InterruptedException, ExecutionException {
		try {
			return get(365, TimeUnit.DAYS);
		}
		catch (TimeoutException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<ServiceResult> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		if (latch.await(timeout, unit)) {
			return results;
		}
		else {
            throw new TimeoutException();
    	}
	}

}
