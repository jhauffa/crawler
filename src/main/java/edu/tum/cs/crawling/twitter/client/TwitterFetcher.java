package edu.tum.cs.crawling.twitter.client;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;

import edu.tum.cs.crawling.twitter.entities.Tweet;
import edu.tum.cs.crawling.twitter.entities.TwitterUser;
import edu.tum.cs.crawling.twitter.protocol.ClientRequestObject;
import edu.tum.cs.crawling.twitter.protocol.ClientRequestObject.ClientRequestType;
import edu.tum.cs.crawling.twitter.protocol.ServerResponseObject.FollowFriendUser;
import edu.tum.cs.crawling.twitter.protocol.ServerResponseObject.FurtherCrawlingUserData;
import edu.tum.cs.util.LanguageDetection;

import twitter4j.IDs;
import twitter4j.Paging;
import twitter4j.RateLimitStatusEvent;
import twitter4j.RateLimitStatusListener;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterFetcher {

	private static final Logger logger = Logger.getLogger(TwitterFetcher.class.getName());

	private static final int FOLLOW_LIMIT = 25000;
	private static final int FRIEND_LIMIT = 25000;
	private static final int WAIT_MS_BETWEEEN_REQUESTS = 1000;

	private long usersDone = 0;
	private long usersSuccessful = 0;
	private long usersError = 0;
	private long usersIgnored = 0;
	private long usersWithTooManyFollowers = 0;
	private long usersWithTooManyFriends = 0;
	private long usersWithTooManyFriendsAndFollowers = 0;
	private long usersSecured = 0;

	private final Twitter twitter;
	private int currentRateLimit = Integer.MAX_VALUE;

	public TwitterFetcher(String consumerKey, String consumerSecret, String token, String tokenSecret) {
		LanguageDetection.loadProfilesFromResources();

		// In case of retweets, Twitter includes the original tweet (the tweet at the beginning of the retweet chain) in
		// the JSON response; the flag "includeRTsEnabled" tells twitter4j to return the original tweet as a separate
		// Status object.
		TwitterFactory factory = new TwitterFactory(new ConfigurationBuilder()
				.setJSONStoreEnabled(true)
				.setIncludeRTsEnabled(true)
				.build());

		twitter = factory.getInstance();
		twitter.setOAuthConsumer(consumerKey, consumerSecret);
		twitter.setOAuthAccessToken(new AccessToken(token, tokenSecret));
		twitter.addRateLimitStatusListener(new RateLimitStatusListener() {
			@Override
			public void onRateLimitStatus(RateLimitStatusEvent evt) {
				setCurrentRateLimit(evt.getRateLimitStatus().getRemaining());
				if (evt.getRateLimitStatus().getRemaining() == 0)
					onRateLimitReached(evt);
			}

			@Override
			public void onRateLimitReached(RateLimitStatusEvent evt) {
				try {
					int sleepingMs = 1000 * (evt.getRateLimitStatus().getSecondsUntilReset() + 30);
					sleepingMs = Math.max(sleepingMs, 60 * 1000);

					GregorianCalendar cal = new GregorianCalendar();
					cal.add(Calendar.MILLISECOND, sleepingMs);

					logger.info("Sleeping now for " + (int) Math.round((sleepingMs / 1000.0 / 60.0)) + "m until " +
							cal.getTime());
					Thread.sleep(sleepingMs);
				} catch (InterruptedException e) {
					// ignore
				}
			}
		});
	}

	private void printStatus() {
		logger.info("usersDone: " + usersDone + " usersSuccessful: " + usersSuccessful +
				" usersError: " + usersError + " usersSecured: " + usersSecured +
				" usersIgnored: " + usersIgnored + " tooManyFollowers:" + usersWithTooManyFollowers +
				" tooManyFriends:" + usersWithTooManyFriends +
				" tooManyFriendsAndFollowers:" + usersWithTooManyFriendsAndFollowers);
	}

	public ClientRequestObject getTweetsAndUsersForIds(Set<Long> userIds, Set<String> screenNames)
			throws InterruptedException {
		Paging tweetsPaging = new Paging(1, 200);
		ClientRequestObject cro = new ClientRequestObject();
		cro.setRequestType(ClientRequestType.DELIVER_TWEETS_AND_USERS);

		for (long userId : userIds) {
			try {
				ResponseList<Status> tweets = twitter.getUserTimeline(userId, tweetsPaging);
				if (tweets.size() > 0) {
					handleTweets(cro, tweets);
				} else {
					TwitterUser user = new TwitterUser();
					user.setId(userId);
					user.setIgnored(true);
					user.setCrawledAt(new Date());
					cro.getUsers().add(user);
				}
			} catch (TwitterException e) {
				if ((e.getCause() == null) && e.getErrorMessage().equals("Not authorized")) {
					TwitterUser user = new TwitterUser();
					user.setId(userId);
					user.setSecured(true);
					user.setCrawledAt(new Date());
					cro.getUsers().add(user);
					usersSecured++;
				} else {
					TwitterUser user = new TwitterUser();
					user.setId(userId);
					user.setCrawlingFailed(true);
					user.setCrawledAt(new Date());
					if (e.getCause() != null) {
						if (e.getCause().getMessage() != null)
							user.setCrawlingFailedCause(e.getCause().getMessage());
						else
							user.setCrawlingFailedCause(e.getClass().getName());
					} else {
						if (e.getMessage() != null)
							user.setCrawlingFailedCause(e.getMessage());
						else
							user.setCrawlingFailedCause(e.getClass().getName());
					}

					logger.warning(user.getCrawlingFailedCause());
					cro.getUsers().add(user);
					usersError++;
				}
			}

			if ((++usersDone % 100) == 0)
				printStatus();
		}

		for (String screenName : screenNames) {
			try {
				ResponseList<Status> tweets = twitter.getUserTimeline(screenName, tweetsPaging);
				if (tweets.size() > 0)
					handleTweets(cro, tweets);
			} catch (TwitterException e) {
				usersError++;
				logger.log(Level.WARNING, "Failed crawling " + screenName);
				cro.getFailedUserScreenNames().add(screenName);
			}

			if ((++usersDone % 100) == 0)
				printStatus();
		}

		return cro;
	}

	private void handleTweets(ClientRequestObject cro, ResponseList<Status> tweets)
			throws TwitterException, InterruptedException {
		Date crawledAt = new Date();
		User user = null;

		// Identifying the language
		String languageCode = LanguageDetection.UNKNOWN_LANGUAGE;
		try {
			Detector detector = DetectorFactory.create();
			for (Status tweet : tweets) {
				detector.append(tweet.getText());
				user = tweet.getUser();
			}
			languageCode = detector.detect();
		} catch (LangDetectException e) {
			// ignore
		}
		if (languageCode.equals(LanguageDetection.UNKNOWN_LANGUAGE))
			languageCode = "-";

		cro.getTweets().addAll(Tweet.fromTwitter4j(tweets, crawledAt));
		TwitterUser internalUser = TwitterUser.fromTwitter4j(user, crawledAt, languageCode);
		Thread.sleep(WAIT_MS_BETWEEEN_REQUESTS);

		if ((user != null) &&
			// need at least 10 tweets for reliable language detection
			(tweets.size() > 10) &&
			// registered for at least 10 days
			user.getCreatedAt().before(new Date(System.currentTimeMillis() - (10 * 24 * 60 * 60 * 1000))) &&
			// most probable language is English
			languageCode.equals("en")) {

			// Followers
			if (user.getFollowersCount() <= FOLLOW_LIMIT) {
				IDs followers = twitter.getFollowersIDs(user.getId(), -1);
				internalUser.getFollowersFriends().addFollowers(followers.getIDs());
				Thread.sleep(WAIT_MS_BETWEEEN_REQUESTS);

				while (followers.hasNext()) {
					followers = twitter.getFollowersIDs(user.getId(), followers.getNextCursor());
					internalUser.getFollowersFriends().addFollowers(followers.getIDs());
					Thread.sleep(WAIT_MS_BETWEEEN_REQUESTS);
				}
			} else {
				usersWithTooManyFollowers++;
			}

			// Friends and followers
			if ((user.getFollowersCount() > FOLLOW_LIMIT) && (user.getFriendsCount() > FRIEND_LIMIT)) {
				usersWithTooManyFriendsAndFollowers++;
			}

			// Friends
			if (user.getFriendsCount() <= FRIEND_LIMIT) {
				IDs friends = twitter.getFriendsIDs(user.getId(), -1);
				internalUser.getFollowersFriends().addFriends(friends.getIDs());
				Thread.sleep(WAIT_MS_BETWEEEN_REQUESTS);

				while (friends.hasNext()) {
					friends = twitter.getFriendsIDs(user.getId(), friends.getNextCursor());
					internalUser.getFollowersFriends().addFriends(friends.getIDs());
					Thread.sleep(WAIT_MS_BETWEEEN_REQUESTS);
				}
			} else {
				usersWithTooManyFriends++;
			}
			usersSuccessful++;
		} else {
			usersIgnored++;
			internalUser.setIgnored(true);
		}

		cro.getUsers().add(internalUser);
	}

	public ClientRequestObject getFurtherTweetsForUserIds(Set<FurtherCrawlingUserData> furtherCrawlingUsers,
			int maxTweetsPerUser, Date firstDate) throws InterruptedException {
		ClientRequestObject cro = new ClientRequestObject();
		cro.setRequestType(ClientRequestType.DELIVER_TWEETS_AND_USERS);

		for (FurtherCrawlingUserData userData : furtherCrawlingUsers) {
			long userId = userData.id;
			long firstTweetId = userData.firstTweetId;
			TwitterUser twitterUser = null;

			try {
				Paging tweetsPaging = new Paging(1);
				tweetsPaging.setMaxId(firstTweetId);
				tweetsPaging.setCount(200);
				Date crawledAt = new Date();

				for (int i = 0; i < (maxTweetsPerUser / tweetsPaging.getCount()); i++) {
					ResponseList<Status> tweets = twitter.getUserTimeline(userId, tweetsPaging);
					cro.getTweets().addAll(Tweet.fromTwitter4j(tweets, crawledAt));
					if (tweets.isEmpty())
						break;

					if ((tweets.get(0) != null) && (twitterUser == null)) {
						twitterUser = TwitterUser.fromTwitter4j(tweets.get(0).getUser(), crawledAt, null);
						cro.getUsers().add(twitterUser);
					}

					Status lastTweet = tweets.get(tweets.size() - 1);
					tweetsPaging.setPage(tweetsPaging.getPage() + 1);
					if (lastTweet.getCreatedAt().before(firstDate))
						break;
					if (tweets.size() < tweetsPaging.getCount()) // No more tweets
						break;

					Thread.sleep(WAIT_MS_BETWEEEN_REQUESTS);
				}
			} catch (TwitterException e) {
				logger.log(Level.INFO, "user " + userId + " is secured?!");
			}
		}
		return cro;
	}

	public ClientRequestObject getFriendsAndFollowers(Set<FollowFriendUser> userIds) throws InterruptedException {
		ClientRequestObject cro = new ClientRequestObject();
		cro.setRequestType(ClientRequestType.DELIVER_FOLLOWERS_AND_FRIENDS);

		for (FollowFriendUser userData : userIds) {
			try {
				TwitterUser user = new TwitterUser();
				user.setId(userData.id);

				// Followers
				if (userData.totalFollowerCount <= FOLLOW_LIMIT) {
					IDs followers = twitter.getFollowersIDs(user.getId(), -1);
					user.getFollowersFriends().addFollowers(followers.getIDs());
					Thread.sleep(WAIT_MS_BETWEEEN_REQUESTS);

					while (followers.hasNext()) {
						followers = twitter.getFollowersIDs(user.getId(), followers.getNextCursor());
						user.getFollowersFriends().addFollowers(followers.getIDs());
						Thread.sleep(WAIT_MS_BETWEEEN_REQUESTS);
					}
				}

				// Friends
				if (userData.totalFriendCount <= FRIEND_LIMIT) {
					IDs friends = twitter.getFriendsIDs(user.getId(), -1);
					user.getFollowersFriends().addFriends(friends.getIDs());
					Thread.sleep(WAIT_MS_BETWEEEN_REQUESTS);

					while (friends.hasNext()) {
						friends = twitter.getFriendsIDs(user.getId(), friends.getNextCursor());
						user.getFollowersFriends().addFriends(friends.getIDs());
						Thread.sleep(WAIT_MS_BETWEEEN_REQUESTS);
					}
				}

				cro.getUsers().add(user);
			} catch (TwitterException e) {
				logger.log(Level.INFO, "user " + userData.id + " is secured?!");
			}
		}
		return cro;
	}

	public int getCurrentRateLimit() {
		return currentRateLimit;
	}

	private void setCurrentRateLimit(int currentRateLimit) {
		this.currentRateLimit = currentRateLimit;
	}

}
