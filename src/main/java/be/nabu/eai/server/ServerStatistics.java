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
