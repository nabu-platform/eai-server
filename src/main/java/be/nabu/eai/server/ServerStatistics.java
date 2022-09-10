package be.nabu.eai.server;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import be.nabu.eai.server.CollaborationListener.User;

@XmlRootElement
public class ServerStatistics {
	private List<User> users;
	private List<ServerStatistic> statistics;
	
	public List<User> getUsers() {
		return users;
	}
	public void setUsers(List<User> users) {
		this.users = users;
	}

	public List<ServerStatistic> getStatistics() {
		return statistics;
	}
	public void setStatistics(List<ServerStatistic> statistics) {
		this.statistics = statistics;
	}

	public static class ServerStatistic {
		private double value;
		private long timestamp;
		private String name;
		private String category;
		
		public ServerStatistic(String category, String name, long timestamp, double value) {
			this.category = category;
			this.name = name;
			this.timestamp = timestamp;
			this.value = value;
		}
		public ServerStatistic() {
			// auto
		}
		public double getValue() {
			return value;
		}
		public void setValue(double value) {
			this.value = value;
		}
		public long getTimestamp() {
			return timestamp;
		}
		public void setTimestamp(long timestamp) {
			this.timestamp = timestamp;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getCategory() {
			return category;
		}
		public void setCategory(String category) {
			this.category = category;
		}
	}
}
