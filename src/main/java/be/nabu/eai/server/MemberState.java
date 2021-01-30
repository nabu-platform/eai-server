package be.nabu.eai.server;

import java.util.Date;

public class MemberState {
	// identity of the member
	private String group, name;
	
	// when the relevant peak started
	private Date cpuPeakStart, memoryPeakStart, cpuPeakReported, memoryPeakReported, lastHeartbeat;

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Date getCpuPeakStart() {
		return cpuPeakStart;
	}

	public void setCpuPeakStart(Date cpuPeakStart) {
		this.cpuPeakStart = cpuPeakStart;
	}

	public Date getMemoryPeakStart() {
		return memoryPeakStart;
	}

	public void setMemoryPeakStart(Date memoryPeakStart) {
		this.memoryPeakStart = memoryPeakStart;
	}

	public Date getCpuPeakReported() {
		return cpuPeakReported;
	}

	public void setCpuPeakReported(Date cpuPeakReported) {
		this.cpuPeakReported = cpuPeakReported;
	}

	public Date getMemoryPeakReported() {
		return memoryPeakReported;
	}

	public void setMemoryPeakReported(Date memoryPeakReported) {
		this.memoryPeakReported = memoryPeakReported;
	}

	public Date getLastHeartbeat() {
		return lastHeartbeat;
	}

	public void setLastHeartbeat(Date lastHeartbeat) {
		this.lastHeartbeat = lastHeartbeat;
	}
}
