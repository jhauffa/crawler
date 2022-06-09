package edu.tum.cs.crawling.twitter.client;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.tum.cs.crawling.twitter.protocol.ClientRequestObject;
import edu.tum.cs.crawling.twitter.protocol.ClientRequestObject.ClientRequestType;
import edu.tum.cs.crawling.twitter.protocol.ServerResponseObject;
import edu.tum.cs.crawling.twitter.protocol.ServerResponseObject.FollowFriendUser;
import edu.tum.cs.crawling.twitter.protocol.ServerResponseObject.FurtherCrawlingUserData;
import edu.tum.cs.crawling.twitter.protocol.ServerResponseObject.ServerStatus;
import edu.tum.cs.util.LogConfigurator;

public class TwitterDumperClient extends LogConfigurator {

	private static final Logger logger = Logger.getLogger(TwitterDumperClient.class.getName());

	private static final int waitingTimeWhileError = 60; // wait for one minute after an error

	private final String host;
	private final int port;

	private final String consumerKey;
	private final String consumerSecret;
	private final String token;
	private final String tokenSecret;

	private static enum FetchMode {
		TWEETS_AND_USERS, FURTHER_TWEETS, FRIENDS_AND_FOLLOWERS
	}
	private final FetchMode fetchMode;

	private final int usersPerRequest;
	private final int maxTweetsPerUser;
	private final Date firstDate;

	public TwitterDumperClient(String host, int port, String consumerKey, String consumerSecret, String token,
			String tokenSecret, FetchMode fetchMode, int usersPerRequest, int maxTweetsPerUser, Date firstDate) {
		this.host = host;
		this.port = port;
		this.consumerKey = consumerKey;
		this.consumerSecret = consumerSecret;
		this.token = token;
		this.tokenSecret = tokenSecret;
		this.fetchMode = fetchMode;
		this.usersPerRequest = usersPerRequest;
		this.maxTweetsPerUser = maxTweetsPerUser;
		this.firstDate = firstDate;
	}

	public void startClient() throws InterruptedException {
		logger.info("Starting client for server " + host + ":" + port);
		TwitterFetcher tf = new TwitterFetcher(consumerKey, consumerSecret, token, tokenSecret);
		while (!Thread.interrupted()) {
			switch (fetchMode) {
			case TWEETS_AND_USERS:
				fetchTweetsAndUsers(tf);
				break;
			case FURTHER_TWEETS:
				fetchFurtherTweets(tf);
				break;
			case FRIENDS_AND_FOLLOWERS:
				fetchFriendsAndFollowers(tf);
				break;
			}
		}
	}

	private abstract class ServerTransaction {
		protected abstract ClientRequestObject processResponse(ServerResponseObject sro) throws InterruptedException;

		private ServerResponseObject sendRequest(ClientRequestObject cro) throws IOException, ClassNotFoundException {
			Socket socket = new Socket(host, port);
			ObjectOutputStream oos = null;
			ObjectInputStream ois = null;
			try {
				oos = new ObjectOutputStream(socket.getOutputStream());
				ois = new ObjectInputStream(socket.getInputStream());
				oos.writeObject(cro);
				oos.flush();
				return (ServerResponseObject) ois.readObject();
			} finally {
				try {
					if (oos != null)
						oos.close();
					if (ois != null)
						ois.close();
				} finally {
					socket.close();
				}
			}
		}

		public void perform(ClientRequestType req, int currentRateLimit) throws InterruptedException {
			try {
				// Request IDs to fetch
				ClientRequestObject cro = new ClientRequestObject();
				// At least one ID and at most userPerRequest or current rate limit / 5, whichever is smaller
				cro.setNumberOfIds(Math.max(Math.min(usersPerRequest, currentRateLimit / 5), 1));
				cro.setRequestType(req);
				logger.info("Requesting " + cro.getNumberOfIds() + " IDs from server");

				ServerResponseObject sro = sendRequest(cro);
				cro = processResponse(sro);

				// Report IDs
				logger.log(Level.INFO, "Reporting " + cro.getUsers().size() + " users and " + cro.getTweets().size() + " tweets");
				sro = sendRequest(cro);
				if (sro.getStatus() != ServerStatus.OK)
					logger.warning("Server returned status " + sro.getStatus());
			} catch (ClassNotFoundException e) {
				logger.log(Level.SEVERE, "Server response class not found, terminate", e);
				System.exit(0);
			} catch (IOException e) {
				logger.log(Level.WARNING, e.getClass().getSimpleName() + ": '" + e.getMessage() + "' retrying in " +
						waitingTimeWhileError + "s");
				Thread.sleep(waitingTimeWhileError * 1000);
			}
		}
	}

	private void fetchFriendsAndFollowers(final TwitterFetcher tf) throws InterruptedException {
		ServerTransaction t = new ServerTransaction() {
			@Override
			protected ClientRequestObject processResponse(ServerResponseObject sro) throws InterruptedException {
				Set<FollowFriendUser> userIds = sro.getFollowFriendsUsers();
				if (userIds.isEmpty()) {
					logger.warning("Got no IDs, exiting");
					System.exit(0);
				}
				logger.info("Got " + userIds.size() + " IDs");
				return tf.getFriendsAndFollowers(userIds);
			}
		};
		t.perform(ClientRequestType.REQUEST_IDS_FOR_FOLLOWER_FRIENDS, tf.getCurrentRateLimit());
	}

	private void fetchTweetsAndUsers(final TwitterFetcher tf) throws InterruptedException {
		ServerTransaction t = new ServerTransaction() {
			@Override
			protected ClientRequestObject processResponse(ServerResponseObject sro) throws InterruptedException {
				Set<Long> userIds = sro.getIds();
				Set<String> screenNames = sro.getScreenNames();
				if (userIds.isEmpty()) {
					logger.warning("Got no IDs, exiting");
					System.exit(0);
				}
				logger.info("Got " + userIds.size() + " IDs and " + screenNames.size() + " screen names");
				return tf.getTweetsAndUsersForIds(userIds, screenNames);
			}
		};
		t.perform(ClientRequestType.REQUEST_IDS, tf.getCurrentRateLimit());
	}

	private void fetchFurtherTweets(final TwitterFetcher tf) throws InterruptedException {
		ServerTransaction t = new ServerTransaction() {
			@Override
			protected ClientRequestObject processResponse(ServerResponseObject sro) throws InterruptedException {
				Set<FurtherCrawlingUserData> furtherCrawlingUsers = sro.getFurtherCrawlingUsers();
				if (furtherCrawlingUsers.isEmpty()) {
					logger.warning("Got no IDs, exiting");
					System.exit(0);
				}
				return tf.getFurtherTweetsForUserIds(furtherCrawlingUsers, maxTweetsPerUser, firstDate);
			}
		};
		t.perform(ClientRequestType.REQUEST_IDS_FOR_FURTHER_TWEETS, tf.getCurrentRateLimit());
	}

	public static void main(String[] args) throws Exception {
		Properties properties = new Properties();
		String configFileName = System.getProperty("edu.tum.cs.crawling.twitter.config.client");
		if (configFileName != null)
			properties.load(new FileInputStream(configFileName));
		else
			properties.load(TwitterDumperClient.class.getResourceAsStream("/twitter.properties"));

		String host = properties.getProperty("server.host");
		int port = Integer.parseInt(properties.getProperty("server.port"));
		int usersPerRequest = Integer.parseInt(properties.getProperty("client.userRequestSize"));

		String consumerKey = properties.getProperty("consumerKey");
		String consumerSecret = properties.getProperty("consumerSecret");
		String token = properties.getProperty("token");
		String tokenSecret = properties.getProperty("tokenSecret");

		if ((consumerKey == null) || consumerKey.isEmpty() ||
			(consumerSecret == null) || consumerSecret.isEmpty() ||
			(token == null) || token.isEmpty() ||
			(tokenSecret == null) || tokenSecret.isEmpty()) {
			logger.severe("Set consumerKey, consumerSecret, token and tokenSecret in twitter.properties.");
			return;
		}

		FetchMode fetchMode = FetchMode.valueOf(properties.getProperty("fetchMode"));

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date firstDate = dateFormat.parse(properties.getProperty("lastDate"));
		int maxTweetsPerUser = Integer.parseInt(properties.getProperty("maxTweetsPerUser"));
		if (maxTweetsPerUser <= 0)
			maxTweetsPerUser = Integer.MAX_VALUE;	// no limit

		TwitterDumperClient tdc = new TwitterDumperClient(host, port, consumerKey, consumerSecret, token, tokenSecret,
				fetchMode, usersPerRequest, maxTweetsPerUser, firstDate);
		tdc.startClient();
	}

}
