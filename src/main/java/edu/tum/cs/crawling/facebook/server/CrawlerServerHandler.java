package edu.tum.cs.crawling.facebook.server;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.tum.cs.crawling.facebook.protocol.ClientRequestObject;
import edu.tum.cs.crawling.facebook.protocol.ServerResponseObject;
import edu.tum.cs.crawling.facebook.protocol.ServerStatistics;

public class CrawlerServerHandler implements Runnable {

	private static final Logger logger = Logger.getLogger(CrawlerServerHandler.class.getName());

	private static final IdBroker idBroker = IdBroker.getInstance();
	private static final WarcArchiver warcArchiver = WarcArchiver.getInstance();
	private static final AtomicInteger pendingProfiles = new AtomicInteger(0);

	private final Socket client;
	private final ServerStatistics statistics;

	public CrawlerServerHandler(Socket client, ServerStatistics statistics) {
		this.client = client;
		this.statistics = statistics;
	}

	private ServerResponseObject processRequest(ClientRequestObject cro) {
		ServerResponseObject sro = new ServerResponseObject();

		// update client statistics
		ServerStatistics.ClientStatistics clientStatistics = null;
		if (cro.getRequestType() != ClientRequestObject.RequestType.REQUEST_STATISTICS) {
			clientStatistics = statistics.getClients().get(client.getInetAddress());
			if (clientStatistics == null) {
				clientStatistics = new ServerStatistics.ClientStatistics();
				statistics.getClients().put(client.getInetAddress(), clientStatistics);
			}
			clientStatistics.updateLastRequestTime();
		}

		// process request
		try {
			switch (cro.getRequestType()) {
			case REQUEST_ID:
				int curPendingProfiles = pendingProfiles.getAndIncrement();
				String id = idBroker.getId();
				if (id != null) {
					logger.info("delivering ID: " + id);
					sro.setId(id);
					sro.setStatus(ServerResponseObject.Status.OK);
				} else if ((curPendingProfiles == 0) && (UserProfileWorker.getNumPendingProfiles() == 0)) {
					logger.info("no more IDs, and no more profiles pending, asking client to terminate");
					pendingProfiles.decrementAndGet();
					sro.setStatus(ServerResponseObject.Status.TERMINATE);
				} else {
					logger.info("no more IDs, asking client to retry later");
					pendingProfiles.decrementAndGet();
					sro.setStatus(ServerResponseObject.Status.RETRY);
				}
				break;
			case REQUEST_STATISTICS:
				logger.info("delivering statistics");
				statistics.setNumPendingProfiles(UserProfileWorker.getNumPendingProfiles());
				sro.setStatistics(statistics);
				sro.setStatus(ServerResponseObject.Status.OK);
				break;
			case DELIVER_PROFILE:
				logger.info("receiving profile data: " + cro.getUserId());
				pendingProfiles.decrementAndGet();
				clientStatistics.incNumProfilesScraped();
				File archive = warcArchiver.savePageSource(cro.getUserId(), cro.getPageSource());
				if (archive != null) {
					UserProfileWorker.submitArchive(archive);
					idBroker.setIdCrawled(cro.getUserId());
					sro.setStatus(ServerResponseObject.Status.OK);
				} else
					sro.setStatus(ServerResponseObject.Status.ERROR);
				break;
			case REPORT_SCRAPE_ERROR:
				logger.info("scrape error for ID: " + cro.getUserId());
				pendingProfiles.decrementAndGet();
				clientStatistics.incNumScrapeErrors();
				Map<String, String> pageSource = cro.getPageSource();
				if (pageSource != null)
					warcArchiver.savePageSource(cro.getUserId(), pageSource);
				idBroker.setIdFailed(cro.getUserId());
				sro.setStatus(ServerResponseObject.Status.OK);
				break;
			case UNKNOWN:
			default:
				logger.warning("unknown request type: " + cro.getRequestType());
				sro.setStatus(ServerResponseObject.Status.ERROR);
				break;
			}
		} catch (Exception e) {
			logger.log(Level.SEVERE, "error handling " + cro.getRequestType() + " request", e);
			sro.setStatus(ServerResponseObject.Status.ERROR);
		}

		return sro;
	}

	@Override
	public void run() {
		try {
			ObjectInputStream ois = new ObjectInputStream(client.getInputStream());
			try {
				ObjectOutputStream oos = new ObjectOutputStream(client.getOutputStream());
				try {
					ClientRequestObject cro = ClientRequestObject.decompress((byte[]) ois.readObject());
					ServerResponseObject sro = processRequest(cro);
					oos.writeObject(sro);
					oos.flush();
				} finally {
					oos.close();
				}
			} finally {
				ois.close();
			}
		} catch (Exception e) {
			logger.log(Level.SEVERE, "error processing request from " + client.getInetAddress(), e);
		} finally {
			try {
				client.close();
			} catch (IOException e) {
				logger.log(Level.WARNING, "error closing client connection", e);
			}
		}
	}

}
