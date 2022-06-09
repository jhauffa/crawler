package edu.tum.cs.crawling.facebook.protocol;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

public class ServerStatistics implements Serializable {

	private static final long serialVersionUID = -3462194279289709431L;

	public static class ClientStatistics implements Serializable {
		private static final long serialVersionUID = 3508757874392386350L;

		private long lastRequestTime;
		private int numProfilesScraped;
		private int numScrapeErrors;

		public long getLastRequestTime() {
			return lastRequestTime;
		}

		public void updateLastRequestTime() {
			lastRequestTime = System.currentTimeMillis();
		}

		public int getNumProfilesScraped() {
			return numProfilesScraped;
		}

		public void incNumProfilesScraped() {
			numProfilesScraped++;
		}

		public int getNumScrapeErrors() {
			return numScrapeErrors;
		}

		public void incNumScrapeErrors() {
			numScrapeErrors++;
		}
	}

	private final long startTime;
	private final Map<InetAddress, ClientStatistics> clients;
	private int numPendingProfiles;

	public ServerStatistics() {
		startTime = System.currentTimeMillis();
		clients = new HashMap<InetAddress, ClientStatistics>();
	}

	public long getStartTime() {
		return startTime;
	}

	public Map<InetAddress, ClientStatistics> getClients() {
		return clients;
	}

	public void setNumPendingProfiles(int numPendingProfiles) {
		this.numPendingProfiles = numPendingProfiles;
	}

	public int getNumPendingProfiles() {
		return numPendingProfiles;
	}

}
