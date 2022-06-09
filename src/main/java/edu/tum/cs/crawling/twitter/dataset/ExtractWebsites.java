package edu.tum.cs.crawling.twitter.dataset;

import de.l3s.boilerpipe.extractors.ArticleExtractor;
import edu.tum.cs.crawling.twitter.entities.Tweet;
import edu.tum.cs.crawling.twitter.entities.Website;
import edu.tum.cs.crawling.twitter.server.TwitterDao;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;

public class ExtractWebsites {

	private static final Logger logger = Logger.getLogger(ExtractWebsites.class.getName());

	private static final AtomicInteger urlCount = new AtomicInteger();
	private static final AtomicInteger badUrlCount = new AtomicInteger();
	private static final List<Long> durations = Collections.synchronizedList(new ArrayList<Long>());
	private static final PoolingHttpClientConnectionManager connectionManager =
			new PoolingHttpClientConnectionManager();
	private static final RequestConfig requestConfig = RequestConfig.custom()
			.setSocketTimeout(15000)
			.setConnectTimeout(15000)
			.setConnectionRequestTimeout(15000)
			.build();
	private static final CloseableHttpClient httpClient = HttpClients.custom()
			.setConnectionManager(connectionManager)
			.setDefaultRequestConfig(requestConfig)
			.build();
	private static final ArticleExtractor extractor = new ArticleExtractor();

	private static int numThreads = 1;
	private static String userAgent = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)";
	private static boolean writeToDatabase = true;

	private static List<Website> extractWebsites(String tweet, long tweetId) {
		List<Website> websites = new ArrayList<Website>();
		Matcher m = ResolveUrls.urlPattern.matcher(tweet);
		while (m.find()) {
			String url = m.group();
			urlCount.incrementAndGet();
			try {
				Website website = startRequest(tweetId, url, url, 0);
				if (website != null)
					websites.add(website);
			} catch (Exception ex) {
				// A lot of errors are already handled in the pipeline, catching the remaining ones here.
				logger.log(Level.SEVERE, "error processing '" + url + "'", ex);
			}
		}
		return websites;
	}

	private static Website startRequest(long ID, String originalUrl, String currentUrl, int numRedirects)
			throws Exception {
		HttpClientContext context = new HttpClientContext();
		HttpGet getRequest = new HttpGet(currentUrl);
		getRequest.addHeader("Accept", "text/*");
		getRequest.addHeader("Accept-Language", "en; q=1.0, *; q=0.2");
		getRequest.addHeader("User-Agent", userAgent);

		Website website = new Website();
		website.setOriginalUrl(originalUrl);
		website.setTweetId(ID);

		try {
			if (numRedirects > 20)
				throw new IOException("Too many redirects");

			HttpResponse response = httpClient.execute(getRequest, context);
			int statusCode = response.getStatusLine().getStatusCode();
			website.setStatusCode(statusCode);

			List<URI> locations = context.getRedirectLocations();
			if (locations != null)
				currentUrl = locations.get(locations.size() - 1).toString();
			website.setResolvedUrl(currentUrl);

			if ((statusCode >= 200) && (statusCode < 300)) {
				// Although we have set the "Accept" header, servers might send us other kinds of data.
				ContentType contentType = ContentType.get(response.getEntity());
				if ((contentType != null) && (contentType.getMimeType() != null) &&
					contentType.getMimeType().startsWith("text")) {
					String body = EntityUtils.toString(response.getEntity());
					String content = extractContent(body);
					website.setContent(content);
				} else
					badUrlCount.incrementAndGet();
			} else if ((statusCode >= 300) && (statusCode < 400)) {
				// HttpClient follows redirects, except for status code 302, which has to be handled manually.
				Header locationHeader = response.getFirstHeader("Location");
				if ((locationHeader != null) && (locationHeader.getValue() != null)) {
					String newUrl = locationHeader.getValue();
					if (!(newUrl.startsWith("http://") || newUrl.startsWith("https://")))
						newUrl = new URL(new URL(currentUrl), newUrl).toString();	// resolve relative URL
					return startRequest(ID, originalUrl, newUrl, numRedirects + 1);
				}
				badUrlCount.incrementAndGet();
			} else {
				badUrlCount.incrementAndGet();
			}
		} catch (IOException ex) {
			List<URI> locations = context.getRedirectLocations();
			if (locations != null)
				currentUrl = locations.get(locations.size() - 1).toString();
			logger.log(Level.SEVERE, "Error fetching content of URL '" + originalUrl + "' (" + currentUrl + ")", ex);
			badUrlCount.incrementAndGet();
			website.setResolvedUrl(currentUrl);
		} finally {
			getRequest.releaseConnection();
		}
		return website;
	}

	private static String extractContent(String html) throws Exception {
		if (html == null)
			return null;
		return extractor.getText(html);
	}

	private static void extractWebsitesForUser(long userID) {
		logger.info("Started " + userID);
		long start = System.currentTimeMillis();
		List<Website> websites = new ArrayList<Website>();
		List<Tweet> tweets = TwitterDao.getTweetsOfUser(userID);
		for (Tweet tweet : tweets) {
			Tweet origTweet = tweet;
			if (tweet.isRetweet())
				origTweet = TwitterDao.getOriginalTweet(tweet);
			websites.addAll(extractWebsites(origTweet.getStatusText(), origTweet.getId()));
			if (writeToDatabase && (websites.size() > 150)) {
				// preventing java.net.SocketException: Too many open files
				TwitterDao.saveWebsites(websites);
				websites.clear();
			}
		}

		if (writeToDatabase && !websites.isEmpty())
			TwitterDao.saveWebsites(websites);

		long durationInSeconds = (System.currentTimeMillis() - start) / 1000;
		durations.add(durationInSeconds);
		logger.info("Finished " + userID + ", took " + durationInSeconds + " seconds on " + tweets.size() + " tweets.");
	}

	public static Set<Long> loadUserIds(String fileName, int maxUsers) throws IOException {
		Set<Long> idList = new HashSet<Long>();
		BufferedReader reader = new BufferedReader(new FileReader(fileName));
		try {
			int numUsers = 0;
			String line;
			while ((line = reader.readLine()) != null) {
				if (line.length() > 0)
					if (idList.add(Long.valueOf(line)))
						if (++numUsers >= maxUsers)
							break;
			}
		} finally {
			reader.close();
		}
		return idList;
	}

	public static void main(String[] args) throws Exception {
		// parse arguments
		String idFileName = null;
		String excludedFileName = null;
		int maxUserIds = Integer.MAX_VALUE;
		int idx = 0;
		while (idx < args.length) {
			if ((args[idx].length() >= 2) && (args[idx].charAt(0) == '-')) {
				switch (args[idx].charAt(1)) {
				case 'n':
					numThreads = Math.max(1, Integer.parseInt(args[++idx]));
					break;
				case 'm':
					maxUserIds = Integer.parseInt(args[++idx]);
					break;
				case 'x':
					excludedFileName = args[++idx];
					break;
				case 'u':
					userAgent = args[++idx];
					break;
				case 't':
					writeToDatabase = false;
					break;
				default:
					System.err.println("usage: " + ExtractWebsites.class.getSimpleName() + " [options] userIds\n" +
							"valid options are:\n" +
							"\t-n x\tNumber of threads (default 1)\n" +
							"\t-m x\tMaximum amount of user IDs to process\n" +
							"\t-x x\tPath of the file with the user IDs to exclude\n" +
							"\t-u x\tThe value of the User-Agent header when sending a request.\n" +
							"\t\tDefault is the header value used by the Google Bot.\n" +
							"\t-t\tDon\'t write to the database.\n");
					return;
				}
			} else
				idFileName = args[idx];
			idx++;
		}
		if (idFileName == null) {
			System.err.println("user ID file name not specified");
			return;
		}

		Set<Long> userIds = loadUserIds(idFileName, maxUserIds);
		if (excludedFileName != null)
			userIds.removeAll(loadUserIds(excludedFileName, Integer.MAX_VALUE));

		logger.info("Processing " + userIds.size() + " users with " + numThreads + " threads.");
		logger.info("User-Agent is '" + userAgent + "'");
		if (!writeToDatabase)
			logger.info("Testing only, not writing to the database.");

		Thread[] threads = new Thread[numThreads];
		final LinkedBlockingQueue<Long> workQueue = new LinkedBlockingQueue<Long>(userIds);
		final PrintWriter finishedIdsFile = new PrintWriter(new FileWriter("finishedIDs"));
		for (int i = 0; i < numThreads; i++) {
			threads[i] = new Thread() {
				@Override
				public void run() {
					Long userId = workQueue.poll();
					if (userId == null)
						return;

					extractWebsitesForUser(userId);

					synchronized (finishedIdsFile) {
						finishedIdsFile.println(userId);
						finishedIdsFile.flush();

						if (durations.size() > 0) {
							double avgTime = 0.0;
							for (long v : durations)
								avgTime += v;
							avgTime /= durations.size();
							logger.info(workQueue.size() + " users to go. Average processing time = " + avgTime);
							logger.info("Estimated remaining time = " + (workQueue.size() * avgTime));
						}
					}
				}
			};
			threads[i].start();
		}
		for (Thread t : threads)
			t.join();
		finishedIdsFile.close();

		logger.info("urlCount: " + urlCount);
		logger.info("badUrlCount: " + badUrlCount);
	}

}
