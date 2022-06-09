package edu.tum.cs.crawling.twitter.dataset;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.Security;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.tum.cs.crawling.twitter.server.TwitterDao;

public class ResolveUrls {

	private static class LinkStatus {
		public int countInTweets;
		public int countAsTarget;
		public int httpStatus;
		public String targetUrl;

		public LinkStatus(boolean fromTweet) {
			if (fromTweet) {
				this.countInTweets = 1;
				this.countAsTarget = 0;
			} else {
				this.countInTweets = 0;
				this.countAsTarget = 1;
			}
			this.httpStatus = -1;
			this.targetUrl = null;
		}
	}

	public static final Pattern urlPattern =
			Pattern.compile("(http|https)\\://[a-zA-Z0-9\\-\\.]+\\.[a-zA-Z]{2,3}(:[a-zA-Z0-9]*)?/?" +
					"([a-zA-Z0-9\\-\\._\\?\\,\\'/\\\\\\+&%\\$#\\=~])*[^\\.\\,\\)\\(\\s\u2018\u2019\u201C\u201D'\"]",
					Pattern.MULTILINE);

	private static int extractUrls(String tweet, Map<String, LinkStatus> links, boolean tcoOnly) {
		int n = 0;
		Matcher m = urlPattern.matcher(tweet);
		while (m.find()) {
			n++;
			String url = m.group();
			if (tcoOnly && !url.startsWith("http://t.co/"))
				continue;

			LinkStatus status = links.get(url);
			if (status == null)
				links.put(url, new LinkStatus(true));
			else
				status.countInTweets++;
		}
		return n;
	}

	/**
	 * Iterate over all Tweets in database; extract URLs and add them to a map.
	 */
	private static Map<String, LinkStatus> extractUrlsFromTweets(int limit, boolean tcoOnly) throws SQLException {
		Map<String, LinkStatus> links = new HashMap<String, LinkStatus>();
		int numURLs = 0;	// URL count before deduplication
		int numTweets = 0;

		Connection dbConn = TwitterDao.getConnection();
		try {
			Statement st = dbConn.createStatement();
			st.setFetchSize(100000);
			ResultSet rs = st.executeQuery("SELECT t1.STATUS_TEXT, t2.STATUS_TEXT FROM TWEET AS t1 " +
					"LEFT JOIN TWEET AS t2 ON t2.ID = t1.RETWEET_OF_STATUS_ID LIMIT " + limit);
			while (rs.next()) {
				String text = rs.getString(2);	// prefer original tweet over retweet, as URLs might have been cut off
				if (text == null)
					text = rs.getString(1);
				numURLs += extractUrls(rs.getString(1), links, tcoOnly);
				numTweets++;
			}
		} finally {
			dbConn.close();
		}

		System.out.println(((float) numURLs / numTweets) + " URLs per Tweet");
		return links;
	}

	private static Map<String, LinkStatus> extractUrlsFromTweetsById(Set<Long> userIdSet, boolean tcoOnly)
			throws SQLException {
		Map<String, LinkStatus> links = new HashMap<String, LinkStatus>();
		int numURLs = 0;	// URL count before deduplication
		int numTweets = 0;

		Connection dbConn = TwitterDao.getConnection();
		try {
			PreparedStatement pst = dbConn.prepareStatement("SELECT t1.STATUS_TEXT, t2.STATUS_TEXT FROM TWEET AS t1 " +
					"LEFT JOIN TWEET AS t2 ON t2.ID = t1.RETWEET_OF_STATUS_ID WHERE t1.USER_ID = ?");
			for (long userId : userIdSet) {
				pst.setLong(1, userId);
				ResultSet rs = pst.executeQuery();
				try {
					while (rs.next()) {
						String text = rs.getString(2);	// prefer original tweet over retweet
						if (text == null)
							text = rs.getString(1);
						numURLs += extractUrls(text, links, tcoOnly);
						numTweets++;
					}
				} finally {
					rs.close();
				}
			}
		} finally {
			dbConn.close();
		}

		System.out.println(((float) numURLs / numTweets) + " URLs per Tweet");
		return links;
	}

	/**
	 * Send HTTP HEAD request for each URL and process the response.
	 */
	private static void resolveUrls(Queue<String> workQueue, Map<String, LinkStatus> links, int limit,
			boolean singlePass, boolean followLocal, boolean printQueueSize) {
		long t0 = System.currentTimeMillis();
		int n = 0;
		while (!workQueue.isEmpty() && (n < limit)) {
			String url = workQueue.poll();

			int httpStatus = -1;
			String targetUrl = null;
			boolean follow = true;

			try {
				URL parsedUrl = new URL(url);
				HttpURLConnection httpConn = (HttpURLConnection) parsedUrl.openConnection();
				httpConn.setRequestMethod("HEAD");
				httpConn.setInstanceFollowRedirects(false);
				httpConn.setConnectTimeout(10 * 1000);
				httpConn.setReadTimeout(10 * 1000);

				httpConn.connect();
				httpStatus = httpConn.getResponseCode();
				if ((httpStatus >= 300) && (httpStatus < 400))
					targetUrl = httpConn.getHeaderField("Location");

				if (targetUrl != null) {
					// resolve relative URL
					if (!(targetUrl.startsWith("http://") || targetUrl.startsWith("https://")))
						targetUrl = new URL(parsedUrl, targetUrl).toString();

					// crawling policy: follow redirects with target on same host?
					if (!followLocal) {
						URL parsedTargetUrl = new URL(targetUrl);
						if (parsedTargetUrl.getHost().equals(parsedUrl.getHost()))
							follow = false;
					}
				}
			} catch (SocketTimeoutException ex) {
				httpStatus = -2;
			} catch (UnknownHostException ex) {
				httpStatus = -3;
			} catch (Exception ex) {
				System.err.println("ERROR: " + ex.getMessage());
			}

			synchronized (links) {
				if (targetUrl != null) {
					LinkStatus targetStatus = links.get(targetUrl);
					if (targetStatus == null) {
						links.put(targetUrl, new LinkStatus(false));
						if (!singlePass && follow)
							workQueue.add(targetUrl);
					} else
						targetStatus.countAsTarget++;
				}

				LinkStatus status = links.get(url);
				status.httpStatus = httpStatus;
				status.targetUrl = targetUrl;
			}

			if ((++n % 100) == 0) {
				long t1 = System.currentTimeMillis();
				float ups = 100 / ((float) (t1 - t0) / 1000);
				t0 = t1;
				if (printQueueSize)
					System.out.println(n + " processed, " + workQueue.size() + " remaining (" + ups + " URLs/s)");
				else
					System.out.println(n + " processed (" + ups + " URLs/s)");
			}
		}
	}

	private static void writeCsv(Map<String, LinkStatus> links, String fileName) throws IOException {
		PrintWriter w = new PrintWriter(fileName);
		try {
			w.println("url\tcountInTweets\tcountAsTarget\thttpStatus\ttargetUrl");
			for (Map.Entry<String, LinkStatus> e : links.entrySet()) {
				LinkStatus status = e.getValue();
				w.println(e.getKey() + "\t" + status.countInTweets + "\t" + status.countAsTarget + "\t" +
						status.httpStatus + "\t" + status.targetUrl);
			}
		} finally {
			w.close();
		}
	}

	public static void main(String[] args) throws Exception {
		String fileName = "links.csv";
		boolean printOnly = false;
		boolean tcoOnly = false;
		boolean singlePass = false;
		boolean followLocal = false;
		int numThreads = 1;
		int tweetLimit = Integer.MAX_VALUE;
		int urlLimit = Integer.MAX_VALUE;
		String idListFileName = null;
		int idListNumUsers = Integer.MAX_VALUE;

		// parse command line arguments
		int idx = 0;
		while (idx < args.length) {
			if ((args[idx].length() >= 2) && (args[idx].charAt(0) == '-')) {
				switch (args[idx].charAt(1)) {
				case 'p':
					printOnly = true;
					break;
				case 't':
					tcoOnly = true;
					break;
				case 's':
					singlePass = true;
					break;
				case 'l':
					followLocal = true;
					break;
				case 'n':
					numThreads = Integer.parseInt(args[++idx]);
					break;
				case 'x':
					tweetLimit = Integer.parseInt(args[++idx]);
					break;
				case 'y':
					urlLimit = Integer.parseInt(args[++idx]);
					break;
				case 'i':
					idListFileName = args[++idx];
					String[] parts = idListFileName.split(":");
					if (parts.length == 2) {
						idListFileName = parts[0];
						idListNumUsers = Integer.parseInt(parts[1]);
					}
					break;
				default:
					System.err.println("usage: " + ResolveUrls.class.getSimpleName() + " [options] [output file]\n" +
							"valid options are:\n" +
							"\t-p\tprint number of extracted URLs, then exit\n" +
							"\t-t\tprocess \"t.co\" URLs only\n" +
							"\t-s\tsingle pass; only process URLs extracted from Tweets\n" +
							"\t-l\tfollow relative redirects and absolute redirects where target is on the same host\n"+
							"\t-n x\tuse x threads (default 1)\n" +
							"\t-x x\textract links from the first x Tweets\n" +
							"\t-y x\tdo not process more than x URLs\n" +
							"\t-i x[:n] \tonly process Tweets by n users from ID list x\n");
					return;
				}
			} else
				fileName = args[idx];
			idx++;
		}

		final Map<String, LinkStatus> links;
		if (idListFileName != null) {
			Set<Long> userIdSet = new HashSet<Long>();
			BufferedReader reader = new BufferedReader(new FileReader(idListFileName));
			try {
				String line;
				while ((userIdSet.size() < idListNumUsers) && ((line = reader.readLine()) != null))
					if (line.length() > 0)
						userIdSet.add(Long.valueOf(line));
			} finally {
				reader.close();
			}

			links = extractUrlsFromTweetsById(userIdSet, tcoOnly);
		} else
			links = extractUrlsFromTweets(tweetLimit, tcoOnly);
		int numOriginalUrls = links.size();
		System.out.println("extracted " + numOriginalUrls + " unique URLs");

		// enable HTTP keep-alive
		System.setProperty("http.keepAlive", "true");
		// enable aggressive DNS caching
		Security.setProperty("networkaddress.cache.ttl", "-1");
		Security.setProperty("networkaddress.cache.negative.ttl", "-1");

		if (!printOnly) {
			long t0 = System.currentTimeMillis();

			if (numThreads == 1) {
				Queue<String> workQueue = new LinkedList<String>(links.keySet());
				resolveUrls(workQueue, links, urlLimit, singlePass, followLocal, true);
			} else {
				final Queue<String> workQueue = new ConcurrentLinkedQueue<String>(links.keySet());
				final int threadUrlLimit = urlLimit;
				final boolean threadSinglePass = singlePass;
				final boolean threadFollowLocal = followLocal;

				Thread[] threads = new Thread[numThreads];
				for (int i = 0; i < numThreads; i++) {
					threads[i] = new Thread(new Runnable() {
						public void run() { resolveUrls(workQueue, links, threadUrlLimit, threadSinglePass,
								threadFollowLocal, false); }
					});
					threads[i].start();
				}
				for (Thread t : threads)
					t.join();
			}

			float ups = Math.min(urlLimit * numThreads, links.size()) /
					((float) (System.currentTimeMillis() - t0) / 1000);
			System.out.println("finished; " + ups + " URLs/s");
		} else if (!tcoOnly) {
			// compute ratio of "t.co" to other hosts
			int numTco = 0;
			int numOthers = 0;
			for (String url : links.keySet()) {
				if (url.startsWith("http://t.co/"))
					numTco++;
				else
					numOthers++;
			}
			System.out.println("t.co ratio = " + ((float) numTco / numOthers));
		}

		System.out.println("redirection factor = " + ((float) links.size() / numOriginalUrls));
		writeCsv(links, fileName);
	}

}
