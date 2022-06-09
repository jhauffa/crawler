package edu.tum.cs.crawling.twitter.server;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import edu.tum.cs.util.LogConfigurator;

public class AddInReplyToData extends LogConfigurator {

	private static final Logger logger = Logger.getLogger(AddInReplyToData.class.getName());

	public static void main(String[] args) throws Exception {
		Connection c = TwitterDao.getConnection();
		try {
			Statement s = c.createStatement();

			// Add ignored users
			Set<Long> ignoredUserIds = new HashSet<Long>();
			ResultSet rs = s.executeQuery("SELECT id FROM user WHERE ignored = 1");
			while (rs.next())
				ignoredUserIds.add(rs.getLong("id"));
			rs.close();
			logger.info("Found " + ignoredUserIds.size() + " ignored userIds");

			PreparedStatement psInsertUserIds = c.prepareStatement(
					"INSERT INTO waiting_user (user_id, screen_name, is_reserved, is_crawled, is_failed," +
					" first_occurrence) VALUES (?,?,0,0,0,?) " +
					"ON DUPLICATE KEY UPDATE first_occurrence = LEAST(first_occurrence, ?)");

			int batchCounter = 0;
			rs = s.executeQuery("SELECT user_id, in_reply_to_user_id, in_reply_to_screenname, crawled_at FROM tweet " +
					"WHERE in_reply_to_user_id > 0");
			while (rs.next()) {
				long user_id = rs.getLong(1);
				if (ignoredUserIds.contains(user_id))
					continue;

				long in_reply_to_user_id = rs.getLong(2);
				String in_reply_to_screen_name = rs.getString(3);
				Timestamp crawledAt = rs.getTimestamp(4);

				psInsertUserIds.setLong(1, in_reply_to_user_id);
				psInsertUserIds.setString(2, in_reply_to_screen_name);
				psInsertUserIds.setTimestamp(3, crawledAt);
				psInsertUserIds.setTimestamp(4, crawledAt);
				psInsertUserIds.addBatch();
				if ((++batchCounter % 10000) == 0) {
					psInsertUserIds.executeBatch();
					logger.info("Executing batch: " + batchCounter);
				}
			}
			psInsertUserIds.executeBatch();
		} finally {
			c.close();
		}
	}

}
