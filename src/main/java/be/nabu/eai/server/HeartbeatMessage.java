package be.nabu.eai.server;

import java.io.Serializable;

public class HeartbeatMessage implements Serializable {
	private static final long serialVersionUID = 1L;

	private String name, group;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}
	
}
