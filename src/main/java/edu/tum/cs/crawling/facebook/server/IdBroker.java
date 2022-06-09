package edu.tum.cs.crawling.facebook.server;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

public class IdBroker {

	private static final Logger logger = Logger.getLogger(IdBroker.class.getName());
	private static IdBroker instance;

	private IdBroker() {
		try {
			Connection c = FacebookDao.getConnection();
			try {
				Statement s = c.createStatement();

				// create table "waiting_user" if it does not yet exist
				s.executeUpdate("CREATE TABLE IF NOT EXISTS `waiting_user` (" +
						"`user_id` VARCHAR(255) NOT NULL, " +
						"`is_reserved` BIT NOT NULL, `is_failed` BIT NOT NULL, `is_crawled` BIT NOT NULL, " +
						"`first_occurrence` TIMESTAMP NOT NULL, " +
						"PRIMARY KEY (`user_id`), " +
						"INDEX `select_index` (`is_reserved` ASC, `is_failed` ASC, `is_crawled` ASC, " +
							"`first_occurrence` ASC)) " +
						"CHARACTER SET latin1 COLLATE latin1_bin");

				// reset existing reservations and failed users
				s.executeUpdate("UPDATE waiting_user SET is_reserved = 0, is_failed = 0");
			} finally {
				c.close();
			}
		} catch (SQLException e) {
			logger.log(Level.SEVERE, "error preparing ID tables in database", e);
		}
	}

	public synchronized String getId() throws SQLException {
		String userId;
		Connection c = FacebookDao.getConnection();
		try {
			PreparedStatement psGetUserId = c.prepareStatement("SELECT user_id FROM waiting_user " +
					"WHERE is_reserved = 0 AND is_crawled = 0 AND is_failed = 0 ORDER BY first_occurrence ASC LIMIT 1");
			ResultSet rs = psGetUserId.executeQuery();
			if (!rs.next())
				return null;
			userId = rs.getString("user_id");

			PreparedStatement psUpdateUserIdReserved = c.prepareStatement(
					"UPDATE waiting_user SET is_reserved = 1 WHERE user_id = ?");
			psUpdateUserIdReserved.setString(1, userId);
			psUpdateUserIdReserved.executeUpdate();
		} finally {
			c.close();
		}
		return userId;
	}

	public void addUserIds(Collection<String> userIds) throws SQLException {
		Connection c = FacebookDao.getConnection();
		try {
			PreparedStatement psInsertUserId = c.prepareStatement("INSERT INTO waiting_user " +
					"(user_id,is_reserved,is_crawled,is_failed,first_occurrence) VALUES (?,0,0,0,NOW()) " +
					"ON DUPLICATE KEY UPDATE user_id = user_id");

			// add user IDs to list if not already present
			for (String userId : userIds) {
				psInsertUserId.setString(1, userId);
				psInsertUserId.addBatch();
			}
			psInsertUserId.executeBatch();
		} finally {
			c.close();
		}
	}

	public void setIdCrawled(String userId) throws SQLException {
		Connection c = FacebookDao.getConnection();
		try {
			PreparedStatement psUpdateUserIdCrawled = c.prepareStatement(
					"UPDATE LOW_PRIORITY waiting_user SET is_crawled = 1 WHERE user_id = ?");
			psUpdateUserIdCrawled.setString(1, userId);
			psUpdateUserIdCrawled.executeUpdate();
		} finally {
			c.close();
		}
	}

	public void setIdFailed(String userId) throws SQLException {
		Connection c = FacebookDao.getConnection();
		try {
			PreparedStatement psUpdateUserIdFailed = c.prepareStatement(
					"UPDATE LOW_PRIORITY waiting_user SET is_failed = 1 WHERE user_id = ?");
			psUpdateUserIdFailed.setString(1, userId);
			psUpdateUserIdFailed.executeUpdate();
		} finally {
			c.close();
		}
	}

	public static synchronized IdBroker getInstance() {
		if (instance == null)
			instance = new IdBroker();
		return instance;
	}

}
