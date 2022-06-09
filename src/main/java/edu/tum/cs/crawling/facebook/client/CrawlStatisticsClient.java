package edu.tum.cs.crawling.facebook.client;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

import edu.tum.cs.crawling.facebook.protocol.ClientRequestObject;
import edu.tum.cs.crawling.facebook.protocol.ServerResponseObject;
import edu.tum.cs.crawling.facebook.protocol.ServerStatistics;
import edu.tum.cs.util.LogConfigurator;

public class CrawlStatisticsClient extends LogConfigurator {

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("usage: " + CrawlStatisticsClient.class.getName() + " <host> <port>");
			return;
		}

		FacebookCrawlerClient c = new FacebookCrawlerClient(args[0], Integer.parseInt(args[1]), null, null);
		ClientRequestObject req = new ClientRequestObject();
		req.setRequestType(ClientRequestObject.RequestType.REQUEST_STATISTICS);
		ServerResponseObject res;
		try {
			res = c.serverRequest(req);
		} catch (IOException ex) {
			System.err.println("error connecting to server");
			ex.printStackTrace();
			return;
		}
		if (res.getStatus() != ServerResponseObject.Status.OK) {
			System.err.println("server returned error: " + res.getStatus());
			return;
		}

		ServerStatistics statistics = res.getStatistics();
		long uptime = (System.currentTimeMillis() - statistics.getStartTime()) / (1000L * 60);
		System.out.println("server uptime: " + uptime + " m");
		System.out.println("processing queue length: " + statistics.getNumPendingProfiles());
		System.out.println("known clients:");
		for (Map.Entry<InetAddress, ServerStatistics.ClientStatistics> e : statistics.getClients().entrySet()) {
			long lastRequest = (System.currentTimeMillis() - e.getValue().getLastRequestTime()) / (1000L * 60);
			System.out.println(e.getKey() + ": seen " + lastRequest + " m ago, # profiles = " +
					e.getValue().getNumProfilesScraped() + " (" +
					((float) e.getValue().getNumProfilesScraped() / uptime) + " per minute), # errors = " +
					e.getValue().getNumScrapeErrors());
		}
	}

}
