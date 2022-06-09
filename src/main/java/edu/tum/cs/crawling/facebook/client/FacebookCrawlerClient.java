package edu.tum.cs.crawling.facebook.client;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.openqa.selenium.remote.UnreachableBrowserException;

import edu.tum.cs.crawling.facebook.protocol.ClientRequestObject;
import edu.tum.cs.crawling.facebook.protocol.ServerResponseObject;
import edu.tum.cs.util.LogConfigurator;

public class FacebookCrawlerClient extends LogConfigurator {

	private static enum CrawlerAction { CONTINUE, RESTART, ABORT };

	private static final Logger logger = Logger.getLogger(FacebookCrawlerClient.class.getName());

	private static final int errorTimeout = 60;
	private static final int maxConsecutiveFailures = 5;

	private final String host;
	private final int port;
	private final String fbUserId;
	private final String fbPassword;

	private int numConsecutiveFailures;

	public FacebookCrawlerClient(String host, int port, String fbUserId, String fbPassword) {
		this.host = host;
		this.port = port;
		this.fbUserId = fbUserId;
		this.fbPassword = fbPassword;

		logger.info("Starting crawler client for server " + host + ":" + port);
	}

	public ServerResponseObject serverRequest(ClientRequestObject req) throws IOException {
		ServerResponseObject res = null;
		// connect to server
		Socket socket = new Socket(host, port);
		try {
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			try {
				ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
				try {
					// send request
					oos.writeObject(ClientRequestObject.compress(req));
					oos.flush();

					// wait for and fetch response
					res = (ServerResponseObject) ois.readObject();
					if (res.getStatus() != ServerResponseObject.Status.OK)
						logger.warning("server returned status " + res.getStatus());
				} catch (ClassNotFoundException e) {
					throw new RuntimeException("server response not understood", e);
				} finally {
					ois.close();
				}
			} finally {
				oos.close();
			}
		} finally {
			socket.close();
		}
		return res;
	}

	private ServerResponseObject serverRequestRetry(ClientRequestObject req) throws InterruptedException {
		ServerResponseObject res = null;
		while (res == null) {
			try {
				res = serverRequest(req);
			} catch (UnknownHostException e) {
				logger.log(Level.SEVERE, "Couldn't reach " + host + ":" + port + ", retrying in " + errorTimeout + "s");
				Thread.sleep(errorTimeout * 1000);
			} catch (IOException e) {
				logger.log(Level.SEVERE, "Error opening socket, retrying in " + errorTimeout + "s", e);
				Thread.sleep(errorTimeout * 1000);
			}
		}
		return res;
	}

	private CrawlerAction fetchProfile(FirefoxCrawler crawler) throws InterruptedException {
		logger.info("requesting ID from server");
		ClientRequestObject req = new ClientRequestObject();
		req.setRequestType(ClientRequestObject.RequestType.REQUEST_ID);
		ServerResponseObject res = serverRequestRetry(req);
		String id;
		if (res.getStatus() == ServerResponseObject.Status.RETRY) {
			logger.warning("server is out of IDs, retrying in " + errorTimeout + "s");
			Thread.sleep(errorTimeout * 1000);
			return CrawlerAction.CONTINUE;
		} else if (res.getStatus() != ServerResponseObject.Status.OK)
			return CrawlerAction.ABORT;
		id = res.getId();
		logger.info("got ID " + id);

		// fetch profile
		Map<String, String> pageSource = null;
		boolean browserCrashed = false;
		try {
			pageSource = crawler.scrapeProfile(id);
		} catch (UnreachableBrowserException ex) {
			// Browser has crashed, abort immediately; due to an unwisely chosen timeout, Selenium takes 3 hours to
			// detect that the browser has become unresponsive, so we definitely do not want to retry (c.f.
			// https://code.google.com/p/selenium/issues/detail?id=3951).
			browserCrashed = true;
		} catch (Exception ex) {
			logger.log(Level.SEVERE, "error while scraping profile", ex);
		}

		if (pageSource != null)
			req.setRequestType(ClientRequestObject.RequestType.DELIVER_PROFILE);
		else
			req.setRequestType(ClientRequestObject.RequestType.REPORT_SCRAPE_ERROR);
		req.setPageSource(pageSource);	// save collected page source if there is any
		req.setUserId(id);

		logger.info("reporting to server");
		res = serverRequestRetry(req);

		// if scraping fails too many times in a row, the account may have been banned, layout may have changed, etc.
		if (pageSource == null) {
			if (browserCrashed) {
				logger.severe("browser does not respond, restarting");
				return CrawlerAction.RESTART;
			} else if (++numConsecutiveFailures >= maxConsecutiveFailures) {
				logger.severe("too many scraping errors, terminating");
				return CrawlerAction.ABORT;
			}
			// TODO: handle connection interruptions (e.g. DSL reconnect with IP change); would probably get a login
			//	page instead of the requested page -> on NoSuchElementException, check for the presence of user/password
			//	text box elements
			// TODO: there should be a way to notify the server, so one can retrieve the list of clients that are no
			//	longer running
		} else
			numConsecutiveFailures = 0;

		return (res.getStatus() == ServerResponseObject.Status.OK) ? CrawlerAction.CONTINUE : CrawlerAction.ABORT;
	}

	private CrawlerAction runClient() throws InterruptedException {
		CrawlerAction action = CrawlerAction.ABORT;
		FirefoxCrawler crawler = new FirefoxCrawler();
		try {
			if (!crawler.login(fbUserId, fbPassword)) {
				logger.severe("could not log in");
				return action;
			}

			while (!Thread.interrupted()) {
				action = fetchProfile(crawler);
				if (action != CrawlerAction.CONTINUE)
					break;
			}
		} finally {
			logger.info("shutting down");
			crawler.shutdown();
		}
		return action;
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.err.println("usage: " + FacebookCrawlerClient.class.getName() + " <host> <port>" +
					" <Facebook user> <Facebook password>");
			return;
		}

		FacebookCrawlerClient c = new FacebookCrawlerClient(args[0], Integer.parseInt(args[1]), args[2], args[3]);
		CrawlerAction action;
		do {
			action = c.runClient();
		} while (action == CrawlerAction.RESTART);
	}

}
