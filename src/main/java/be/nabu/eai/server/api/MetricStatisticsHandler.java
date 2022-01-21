package be.nabu.eai.server.api;

import java.util.List;

import javax.jws.WebParam;
import javax.jws.WebResult;

import be.nabu.eai.repository.util.MetricStatistics;

public interface MetricStatisticsHandler {
	@WebResult(name = "handled")
	public boolean handle(@WebParam(name = "metrics") List<MetricStatistics> events);
}
