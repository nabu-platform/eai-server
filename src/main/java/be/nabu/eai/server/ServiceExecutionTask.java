package be.nabu.eai.server;

import java.io.Serializable;

public class ServiceExecutionTask implements Serializable {
	
	private static final long serialVersionUID = -172049359373071069L;

	private String serviceId, target, input, runId;

	public String getRunId() {
		return runId;
	}
	public void setRunId(String runId) {
		this.runId = runId;
	}
	public String getServiceId() {
		return serviceId;
	}
	public void setServiceId(String serviceId) {
		this.serviceId = serviceId;
	}
	public String getInput() {
		return input;
	}
	public void setInput(String input) {
		this.input = input;
	}
	public String getTarget() {
		return target;
	}
	public void setTarget(String target) {
		this.target = target;
	}
}
