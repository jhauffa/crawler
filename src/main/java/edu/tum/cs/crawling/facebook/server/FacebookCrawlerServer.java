package edu.tum.cs.crawling.facebook.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.tum.cs.crawling.facebook.protocol.ServerStatistics;
import edu.tum.cs.util.LanguageDetection;
import edu.tum.cs.util.LogConfigurator;

public class FacebookCrawlerServer extends LogConfigurator {

	private static final Logger logger = Logger.getLogger(FacebookCrawlerServer.class.getName());

	private final int port;

	public FacebookCrawlerServer(int port) {
		this.port = port;
	}

	public void runServer() {
		UserProfileWorker.submitArchives(WarcArchiver.getInstance().getUnprocessedArchives());
		UserProfileWorker profileWorker = new UserProfileWorker();
		Thread profileWorkerThread = new Thread(profileWorker);
		profileWorkerThread.start();

		ServerStatistics statistics = new ServerStatistics();
		ExecutorService pool = Executors.newCachedThreadPool();
		ServerSocket serverSocket = null;
		try {
			serverSocket = new ServerSocket(port);
			while (!Thread.interrupted()) {
				Socket cs = serverSocket.accept();
				pool.execute(new CrawlerServerHandler(cs, statistics));
			}
		} catch (IOException ex) {
			logger.log(Level.SEVERE, "IO error", ex);
		} finally {
			try {
				logger.info("waiting for user profile processing to finish");
				profileWorkerThread.interrupt();
				profileWorkerThread.join();

				logger.info("shutting down server");
				pool.shutdown();
				pool.awaitTermination(4L, TimeUnit.SECONDS);
				if ((serverSocket != null) && !serverSocket.isClosed())
					serverSocket.close();
			} catch (Exception ex) {
				logger.log(Level.WARNING, "error while shutting down server", ex);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.err.println("usage: " + FacebookCrawlerServer.class.getName() + " <DB host> <DB name>" +
					" <DB user> <DB password> [<Facebook user ID> ...]");
			return;
		}

		LanguageDetection.loadProfilesFromResources();
		FacebookDao.initialize(args[0], args[1], args[2], args[3]);
		try {
			// add seed user IDs if specified
			int numSeedUsers = Math.max(0, args.length - 4);
			if (numSeedUsers > 0) {
				List<String> seedUserIds = new ArrayList<String>(numSeedUsers);
				for (int i = 4; i < args.length; i++)
					seedUserIds.add(args[i]);
				IdBroker.getInstance().addUserIds(seedUserIds);
			}

			FacebookCrawlerServer s = new FacebookCrawlerServer(3141);
			s.runServer();
		} finally {
			FacebookDao.shutdown();
		}
	}

}
