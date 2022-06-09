package edu.tum.cs.crawling.twitter.server;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import edu.tum.cs.crawling.twitter.entities.Tweet;
import edu.tum.cs.crawling.twitter.entities.TwitterUser;
import edu.tum.cs.util.LogConfigurator;

/**
 * Append users to the list of users with earlier tweets that have yet to be crawled.
 */
public class AddWaitingForFurtherCrawlingUsers extends LogConfigurator {

	private static final Logger logger = Logger.getLogger(AddWaitingForFurtherCrawlingUsers.class.getName());

	public static void main(String[] args) throws Exception {
		Connection c = TwitterDao.getConnection();
		try {
			PreparedStatement psInsert = c.prepareStatement(
					"INSERT DELAYED INTO waiting_user_for_further_tweets (user_id, first_tweet_date, first_tweet_id," +
					" last_tweet_date, last_tweet_id, is_reserved, is_crawled, has_more_tweets) " +
					"VALUES (?, ?, ?, ?, ?, 0, 0, ?)");
			PreparedStatement psGetRetweets = c.prepareStatement(
					"SELECT retweet_of_status_id FROM tweet WHERE retweet_of_user_id = ?");

			Statement s = c.createStatement();
			Set<Long> existingUserIds = new HashSet<Long>();
			ResultSet rs = s.executeQuery("SELECT user_id FROM waiting_user_for_further_tweets");
			while (rs.next()) {
				long userId = rs.getLong(1);
				existingUserIds.add(userId);
			}
			rs.close();

			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-dd-MM Z");
			java.util.Date startCrawlingDate = dateFormat.parse("2012-01-01 -0000"); // GMT
			logger.info("First date is " + startCrawlingDate.toString() +
					" - existingUserIds.size = " + existingUserIds.size());

			int i = 0;
			Set<Long> retweetedTweetsOfUser = new HashSet<Long>();
			rs = s.executeQuery("SELECT id FROM user WHERE ignored = 0 AND secured = 0 AND crawling_failed = 0");
			logger.info("Starting");
			while (rs.next()) {
				long userId = rs.getLong(1);
				if (existingUserIds.contains(userId))
					continue;
				TwitterUser user = TwitterDao.getUser(userId);

				retweetedTweetsOfUser.clear();
				psGetRetweets.setLong(1, user.getId());
				ResultSet retweetsRS = psGetRetweets.executeQuery();
				while (retweetsRS.next()) {
					long statusId = retweetsRS.getLong(1);
					retweetedTweetsOfUser.add(statusId);
				}
				retweetsRS.close();

				Tweet firstTweet = null;
				Tweet lastTweet = null;

				Collection<Tweet> tweets = TwitterDao.getTweetsOfUser(user.getId());
				for (Tweet tweet : tweets) {
					// Retweeted tweets could be added with the tweets of another user; ignore them
					if (retweetedTweetsOfUser.contains(tweet.getId()))
						continue;

					if ((firstTweet == null) || firstTweet.getCreatedAt().after(tweet.getCreatedAt()))
						firstTweet = tweet;

					if ((lastTweet == null) || firstTweet.getCreatedAt().before(tweet.getCreatedAt()))
						lastTweet = tweet;
				}

				boolean hasMoreTweets = false;
				if (tweets.size() >= 200) {
					if (firstTweet == null) { // Only retweets
						hasMoreTweets = true;
					} else if (firstTweet.getCreatedAt().after(startCrawlingDate) &&
							   (user.getTotalTweetCount() > tweets.size())) {
						hasMoreTweets = true;
					}
				}

				int j = 1;
				psInsert.setLong(j++, user.getId());
				if (firstTweet != null) {
					psInsert.setTimestamp(j++, new Timestamp(firstTweet.getCreatedAt().getTime()));
					psInsert.setLong(j++, firstTweet.getId());
				} else {
					psInsert.setTimestamp(j++, null);
					psInsert.setLong(j++, 0);
				}
				if (lastTweet != null) {
					psInsert.setTimestamp(j++, new Timestamp(lastTweet.getCreatedAt().getTime()));
					psInsert.setLong(j++, lastTweet.getId());
				} else {
					psInsert.setTimestamp(j++, null);
					psInsert.setLong(j++, 0);
				}
				psInsert.setBoolean(j++, hasMoreTweets);

				psInsert.addBatch();
				if ((++i % 100) == 0) {
					if ((i % 1000) == 0)
						logger.info("Executing batch " + i);
					psInsert.executeBatch();
				}
			}
			psInsert.executeBatch();
			logger.info("Executing batch " + i);
		} finally {
			c.close();
		}
	}

}
