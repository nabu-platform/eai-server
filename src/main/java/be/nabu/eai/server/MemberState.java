package be.nabu.eai.server;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import be.nabu.libs.metrics.core.sinks.LimitedHistorySinkWithStatistics;

public class MemberState {
	
	// the interval (in seconds) that we expect a heartbeat
	public static final int INTERVAL = 5;
	
	// the amount of missed heartbeats that are allowed before we raise the alarms
	public static final int MISSED_BEATS = 3;
	
	// the amount of heartbeats we want to keep track of
	// by default we would prefer at least 30 minutes and if possible more, seeing as there are 20 heartbeats per minute and 30 minutes...
	public static final int WINDOW = 600;
	
	
	// identity of the member
	private String group, name;
	
	// when the relevant peak started, we don't want to do repeat warnings in the same peak
	// we also keep track of when we reported it, so we don't report it multiple times
	private Date cpuPeakStart, memoryPeakStart, cpuPeakReported, memoryPeakReported, lastHeartbeat,
		fileDescriptorReported;
	
	// keep track of file system fullage :|
	private Map<String, Date> fileSystemReported = new HashMap<String, Date>();
	
	private LimitedHistorySinkWithStatistics cpuSink = new LimitedHistorySinkWithStatistics(WINDOW / INTERVAL);
	private LimitedHistorySinkWithStatistics memorySink = new LimitedHistorySinkWithStatistics(WINDOW / INTERVAL);
	private LimitedHistorySinkWithStatistics fileDescriptorSink = new LimitedHistorySinkWithStatistics(WINDOW / INTERVAL);

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
