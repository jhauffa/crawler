package edu.tum.cs.crawling.twitter.server;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.tum.cs.crawling.twitter.entities.Tweet;
import edu.tum.cs.crawling.twitter.entities.TwitterUser;
import edu.tum.cs.crawling.twitter.protocol.ClientRequestObject;
import edu.tum.cs.crawling.twitter.protocol.ServerResponseObject;
import edu.tum.cs.crawling.twitter.protocol.ServerResponseObject.FollowFriendUser;
import edu.tum.cs.crawling.twitter.protocol.ServerResponseObject.FurtherCrawlingUserData;
import edu.tum.cs.crawling.twitter.protocol.ServerResponseObject.ServerStatus;

public class IdBroker {

	private static final Logger logger = Logger.getLogger(IdBroker.class.getName());

	private static final Pattern USER_NAME_PATTERN = Pattern.compile("@([a-zA-Z0-9_\\-]+)");

	private static IdBroker singleton;

	/**
	 * Never close this connection!
	 */
	private Connection c;

	private PreparedStatement psInsertUserIds;
	private PreparedStatement psUpdateUserIdReserved;
	private PreparedStatement psUpdateUserIdCrawlingFurtherReserved;
	private PreparedStatement psUpdateUserIdScreenNameCrawled;
	private PreparedStatement psUpdateUserIdFailed;
	private PreparedStatement psUpdateScreenNameReserved;
	private PreparedStatement psUpdateScreenNameFailed;
	private PreparedStatement psSelectUsersForFriendsAndFollowers;

	private Set<String> knownScreenNames = new HashSet<String>();
	private Set<Long> knownUserIds = new HashSet<Long>();
	private Set<Long> ignoredUserIds = new HashSet<Long>();
	private Set<Long> usersIdsForFriendsAndFollowersOut = new HashSet<Long>();

	private IdBroker() {
		try {
			c = TwitterDao.getConnection();
			Statement s = c.createStatement();

			// create table "waiting_user" if it does not yet exist
			s.executeUpdate("CREATE TABLE IF NOT EXISTS `waiting_user` (" +
					"`user_id` bigint(20) DEFAULT NULL," +
					"`screen_name` varchar(255) DEFAULT NULL," +
					"`is_reserved` bit(1) NOT NULL," +
					"`is_crawled` bit(1) NOT NULL," +
					"`is_failed` bit(1) NOT NULL," +
					"`first_occurrence` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," +
					"UNIQUE KEY `user_id_UNIQUE` (`user_id`)," +
					"UNIQUE KEY `screen_name_UNIQUE` (`screen_name`)," +
					"KEY `select_index` (`is_reserved`,`is_crawled`,`is_failed`,`first_occurrence`)) " +
					"CHARACTER SET latin1 COLLATE latin1_bin");
			// create table "waiting_user_for_further_tweets" if it does not yet exist
			s.executeUpdate("CREATE TABLE IF NOT EXISTS `waiting_user_for_further_tweets` (" +
					"`user_id` bigint(20) NOT NULL," +
					"`first_tweet_date` datetime DEFAULT NULL," +
					"`first_tweet_id` bigint(20) DEFAULT NULL," +
					"`last_tweet_date` datetime DEFAULT NULL," +
					"`last_tweet_id` bigint(20) DEFAULT NULL," +
					"`is_reserved` bit(1) DEFAULT NULL," +
					"`is_crawled` bit(1) DEFAULT NULL," +
					"`has_more_tweets` bit(1) DEFAULT NULL," +
					"PRIMARY KEY `PRIMARY` (`user_id`)) " +
					"CHARACTER SET latin1 COLLATE latin1_bin");

			// Load existing user_ids and screen_names
			ResultSet rs = s.executeQuery("SELECT id, screen_name FROM user");
			while (rs.next()) {
				knownUserIds.add(rs.getLong(1));
				if (rs.getString(2) != null)
					knownScreenNames.add(rs.getString(2).toLowerCase());
			}
			rs.close();
			logger.info("Found " + knownUserIds.size() + " user IDs and " + knownScreenNames.size() +
					" screen names in 'USER'");

			// Add uncrawled users
			rs = s.executeQuery("SELECT user_id, screen_name FROM waiting_user");
			while (rs.next()) {
				long userId = rs.getLong(1);
				String screenName = rs.getString(2);
				if (userId != 0)
					knownUserIds.add(userId);
				if ((screenName != null) && !screenName.isEmpty())
					knownScreenNames.add(screenName.toLowerCase());
			}
			rs.close();
			logger.info("Found " + knownUserIds.size() + " user IDs and " + knownScreenNames.size() +
					" screen names in 'waiting_user'");

			// Add ignored users
			rs = s.executeQuery("SELECT id FROM user WHERE ignored = 1");
			while (rs.next())
				ignoredUserIds.add(rs.getLong(1));
			rs.close();
			logger.info("Found " + ignoredUserIds.size() + " ignored userIds");

			// Reset existing reservations
			s.execute("UPDATE waiting_user SET is_reserved = 0 WHERE is_reserved = 1 AND is_crawled = 0");
			// Reset failed users
			s.execute("UPDATE waiting_user SET is_failed = 0, is_crawled = 0, is_reserved = 0 " +
					"WHERE user_id IS NOT NULL AND is_failed = 1");

			psInsertUserIds = c.prepareStatement(
					"INSERT DELAYED INTO waiting_user (user_id, screen_name, is_reserved, is_crawled, is_failed," +
					" first_occurrence) VALUES (?,?,0,0,0,NOW())");

			psUpdateUserIdCrawlingFurtherReserved = c.prepareStatement(
					"UPDATE waiting_user_for_further_tweets SET is_reserved = 1 WHERE user_id = ?");

			psUpdateUserIdReserved = c.prepareStatement(
					"UPDATE waiting_user SET is_reserved = 1 WHERE user_id = ?");
			psUpdateUserIdScreenNameCrawled = c.prepareStatement(
					"UPDATE LOW_PRIORITY waiting_user SET is_crawled = 1 WHERE user_id = ? OR" +
					" (screen_name = ? AND screen_name IS NOT NULL)");
			psUpdateUserIdFailed = c.prepareStatement(
					"UPDATE LOW_PRIORITY waiting_user SET is_failed = 1 WHERE user_id = ?");

			psUpdateScreenNameReserved = c.prepareStatement(
					"UPDATE waiting_user SET is_reserved = 1 WHERE screen_name = ?");
			psUpdateScreenNameFailed = c.prepareStatement(
					"UPDATE LOW_PRIORITY waiting_user SET is_failed = 1 WHERE screen_name = ?");

			psSelectUsersForFriendsAndFollowers = c.prepareStatement(
					"SELECT id, total_follower_count, total_friend_count FROM user " +
					"WHERE ignored = 0 AND secured = 0 AND crawling_failed = 0 AND friends_ser IS NULL AND" +
					" followers_ser IS NULL AND (total_follower_count < 25000 OR total_friend_count < 25000) " +
					"ORDER BY RAND() LIMIT ?");

			s.close();
		} catch (SQLException e) {
			logger.log(Level.SEVERE, "Error getting existing IDs and screen names", e);
		}
	}

	public synchronized ServerResponseObject getIds(int numberOfIds) throws SQLException {
		ServerResponseObject sro = new ServerResponseObject();

		Statement s = c.createStatement();
		ResultSet rs = s.executeQuery("SELECT user_id, screen_name FROM waiting_user " +
				"WHERE is_reserved = 0 AND is_crawled = 0 AND is_failed = 0 " +
				"ORDER BY first_occurrence ASC LIMIT " + numberOfIds);
		while (rs.next()) {
			long userId = rs.getLong(1);
			String screenName = rs.getString(2);
			if (userId == 0)
				sro.getScreenNames().add(screenName);
			else
				sro.getIds().add(userId);
		}
		rs.close();
		s.close();

		if (sro.getIds().size() > 0) {
			for (Long userId : sro.getIds()) {
				psUpdateUserIdReserved.setLong(1, userId);
				psUpdateUserIdReserved.addBatch();
			}
			psUpdateUserIdReserved.executeBatch();
		}

		if (sro.getScreenNames().size() > 0) {
			for (String screenName : sro.getScreenNames()) {
				psUpdateScreenNameReserved.setString(1, screenName);
				psUpdateScreenNameReserved.addBatch();
			}
			psUpdateScreenNameReserved.executeBatch();
		}

		sro.setStatus(ServerStatus.DELIVER_IDS);
		return sro;
	}

	public synchronized ServerResponseObject getIdsForFurtherCrawling(int numberOfIds) throws SQLException {
		ServerResponseObject sro = new ServerResponseObject();

		Statement s = c.createStatement();
		ResultSet rs = s.executeQuery("SELECT user_id, first_tweet_id FROM waiting_user_for_further_tweets " +
				"WHERE is_reserved = 0 AND is_crawled = 0 AND has_more_tweets = 1 " +
				"ORDER BY RAND() LIMIT " + numberOfIds);
		while (rs.next()) {
			long userId = rs.getLong(1);
			long firstTweetId = rs.getLong(2);

			FurtherCrawlingUserData userData = new FurtherCrawlingUserData();
			userData.id = userId;
			userData.firstTweetId = firstTweetId;

			sro.getFurtherCrawlingUsers().add(userData);
		}
		rs.close();
		s.close();

		if (sro.getFurtherCrawlingUsers().size() > 0) {
			for (FurtherCrawlingUserData userData : sro.getFurtherCrawlingUsers()) {
				psUpdateUserIdCrawlingFurtherReserved.setLong(1, userData.id);
				psUpdateUserIdCrawlingFurtherReserved.addBatch();
			}
			psUpdateUserIdCrawlingFurtherReserved.executeBatch();
		}

		return sro;
	}

	public synchronized ServerResponseObject getIdsForFollowerFriends(int numberOfIds) throws SQLException {
		ServerResponseObject sro = new ServerResponseObject();

		psSelectUsersForFriendsAndFollowers.setInt(1, Math.max(2 * numberOfIds, 50));
		ResultSet rs = psSelectUsersForFriendsAndFollowers.executeQuery();
		while (rs.next()) {
			long userId = rs.getLong(1);
			if (usersIdsForFriendsAndFollowersOut.contains(userId))
				continue;
			if (sro.getFollowFriendsUsers().size() >= numberOfIds) // Enough IDs
				break;
			usersIdsForFriendsAndFollowersOut.add(userId);

			FollowFriendUser userData = new FollowFriendUser();
			userData.id = userId;
			userData.totalFollowerCount = rs.getInt(2);
			userData.totalFriendCount = rs.getInt(3);
			sro.getFollowFriendsUsers().add(userData);
		}
		rs.close();

		return sro;
	}

	public void extractIds(ClientRequestObject cro, boolean addIdsToWaitingList) throws SQLException {
		// Update and add existing user ID
		Set<Long> failedUserIds = new HashSet<Long>();
		for (TwitterUser user : cro.getUsers()) {
			psUpdateUserIdScreenNameCrawled.setLong(1, user.getId());
			psUpdateUserIdScreenNameCrawled.setString(2, user.getScreenName());
			psUpdateUserIdScreenNameCrawled.addBatch();
			if (user.isIgnored())
				ignoredUserIds.add(user.getId());
			knownUserIds.add(user.getId());
			if (user.getScreenName() != null)
				knownScreenNames.add(user.getScreenName().toLowerCase());
			if (user.isCrawlingFailed())
				failedUserIds.add(user.getId());
		}
		psUpdateUserIdScreenNameCrawled.executeBatch();

		if (failedUserIds.size() > 0) {
			for (long failedUserId : failedUserIds) {
				psUpdateUserIdFailed.setLong(1, failedUserId);
				psUpdateUserIdFailed.addBatch();
			}
			psUpdateUserIdFailed.executeBatch();
		}

		// Update and add existing screen names
		if (cro.getFailedUserScreenNames().size() > 0) {
			for(String failedScreenName : cro.getFailedUserScreenNames()) {
				psUpdateScreenNameFailed.setString(1, failedScreenName);
				psUpdateScreenNameFailed.addBatch();
				knownScreenNames.add(failedScreenName.toLowerCase());
			}
			psUpdateScreenNameFailed.execute();
		}

		if (!addIdsToWaitingList)
			return;

		for (TwitterUser user : cro.getUsers()) {
			if (user.isIgnored())
				continue;

			// Add followers
			for (long userId : user.getFollowersFriends().getFollowers()) {
				if (!knownUserIds.contains(userId)) {
					knownUserIds.add(userId);
					psInsertUserIds.setLong(1, userId);
					psInsertUserIds.setString(2, null);
					psInsertUserIds.addBatch();
				}
			}
			// Add friends
			for (long userId : user.getFollowersFriends().getFriends()) {
				if (!knownUserIds.contains(userId)) {
					knownUserIds.add(userId);
					psInsertUserIds.setLong(1, userId);
					psInsertUserIds.setString(2, null);
					psInsertUserIds.addBatch();
				}
			}
		}

		for (Tweet tweet : cro.getTweets()) {
			if (ignoredUserIds.contains(tweet.getUserId()))
				continue;

			// Add retweets
			if (tweet.isRetweet()) {
				if (!knownUserIds.contains(tweet.getRetweetOfUserId()) &&
					!knownScreenNames.contains(tweet.getRetweetOfScreenName().toLowerCase())) {
					psInsertUserIds.setLong(1, tweet.getRetweetOfUserId());
					psInsertUserIds.setString(2, tweet.getRetweetOfScreenName());
					psInsertUserIds.addBatch();
				}
				knownUserIds.add(tweet.getRetweetOfUserId());
				knownScreenNames.add(tweet.getRetweetOfScreenName().toLowerCase());
			}

			// Add message information
			if (tweet.getInReplyToUserId() > 0) {
				if (!knownUserIds.contains(tweet.getInReplyToUserId()) &&
					!knownScreenNames.contains(tweet.getInReplyToScreenname().toLowerCase())) {
					psInsertUserIds.setLong(1, tweet.getInReplyToUserId());
					psInsertUserIds.setString(2, tweet.getInReplyToScreenname());
					psInsertUserIds.addBatch();
				}
				knownUserIds.add(tweet.getInReplyToUserId());
				knownScreenNames.add(tweet.getInReplyToScreenname().toLowerCase());
			}

			// Add @mentions
			for (String atMentionedScreenName : filterByPattern(tweet.getStatusText(), USER_NAME_PATTERN)) {
				atMentionedScreenName = atMentionedScreenName.substring(1); // Remove @
				if (!knownScreenNames.contains(atMentionedScreenName.toLowerCase())) {
					knownScreenNames.add(atMentionedScreenName.toLowerCase());
					psInsertUserIds.setNull(1, Types.BIGINT);
					psInsertUserIds.setString(2, atMentionedScreenName);
					psInsertUserIds.addBatch();
				}
			}
		}
		psInsertUserIds.executeBatch();
	}

	private static Set<String> filterByPattern(String status, Pattern pattern) {
		Matcher hashtags = pattern.matcher(status);
		Set<String> rCol = new HashSet<String>();
		while (hashtags.find())
			rCol.add(hashtags.group());
		return rCol;
	}

	public static IdBroker getInstance() {
		if (singleton == null)
			singleton = new IdBroker();
		return singleton;
	}

}
