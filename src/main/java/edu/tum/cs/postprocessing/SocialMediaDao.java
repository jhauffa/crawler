package edu.tum.cs.postprocessing;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.mchange.v2.c3p0.DataSources;

public class SocialMediaDao {

	private static final Logger logger = Logger.getLogger(SocialMediaDao.class.getName());

	private final String tablePrefix;
	private final DataSource pooledDataSource;

	public SocialMediaDao(String databaseIp, String databaseName, String userName, String password, String tablePrefix)
			throws ClassNotFoundException, SQLException {
		this.tablePrefix = tablePrefix;

		// connect to database
		String url = "jdbc:mysql://" + databaseIp + "/" + databaseName;
		DataSource unpooledDataSource = DataSources.unpooledDataSource(url, userName, password);
		pooledDataSource = DataSources.pooledDataSource(unpooledDataSource);

		// create tables if they do not yet exist
		createTables();
	}

	public void shutdown() throws SQLException {
		DataSources.destroy(pooledDataSource);
	}

	private void createTables() throws SQLException {
		Connection conn = getConnection();
		try {
			Statement s = conn.createStatement();
			s.executeUpdate("CREATE TABLE IF NOT EXISTS `" + tablePrefix + "_USER` (" +
					"`id` int(10) unsigned NOT NULL AUTO_INCREMENT," +
					"`nativeId` varchar(256) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL," +
					"`name` varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
					"`isFullyCrawled` boolean NOT NULL," +
					"`numRelationships` int(10) unsigned NOT NULL," +
					"PRIMARY KEY (`id`), UNIQUE KEY (`nativeId`))");
			s.executeUpdate("CREATE TABLE IF NOT EXISTS `" + tablePrefix + "_RELATIONSHIPS` (" +
					"`from` int(10) unsigned NOT NULL," +
					"`to` int(10) unsigned NOT NULL," +
					"PRIMARY KEY (`from`,`to`)," +
					"FOREIGN KEY (`from`) REFERENCES `" + tablePrefix + "_USER` (`id`)," +
					"FOREIGN KEY (`to`) REFERENCES `" + tablePrefix + "_USER` (`id`))");
			s.executeUpdate("CREATE TABLE IF NOT EXISTS `" + tablePrefix + "_MESSAGE` (" +
					"`id` int(10) unsigned NOT NULL AUTO_INCREMENT," +
					"`nativeId` varchar(256) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL," +
					"`date` datetime NOT NULL," +
					"`sender` int(10) unsigned NOT NULL," +
					"`isReply` boolean NOT NULL," +
					"`parent` int(10) unsigned DEFAULT NULL," +
					"`isShared` boolean NOT NULL," +
					"`origin` int(10) unsigned DEFAULT NULL," +
					"`content` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL," +
					"PRIMARY KEY (`id`), UNIQUE KEY (`nativeId`)," +
					"FOREIGN KEY (`sender`) REFERENCES `" + tablePrefix + "_USER` (`id`)," +
					"FOREIGN KEY (`parent`) REFERENCES `" + tablePrefix + "_MESSAGE` (`id`)," +
					"FOREIGN KEY (`origin`) REFERENCES `" + tablePrefix + "_MESSAGE` (`id`))");
			s.executeUpdate("CREATE TABLE IF NOT EXISTS `" + tablePrefix + "_RECIPIENTS` (" +
					"`message` int(10) unsigned NOT NULL," +
					"`recipient` int(10) unsigned NOT NULL," +
					"PRIMARY KEY (`message`,`recipient`)," +
					"FOREIGN KEY (`message`) REFERENCES `" + tablePrefix + "_MESSAGE` (`id`)," +
					"FOREIGN KEY (`recipient`) REFERENCES `" + tablePrefix + "_USER` (`id`))");
		} finally {
			conn.close();
		}
	}

	public Connection getConnection() throws SQLException {
		return pooledDataSource.getConnection();
	}

	private static String truncateString(String s, int maxLength, String field) {
		if (s.length() > maxLength) {
			logger.warning("truncating value of '" + field + "' from " + s.length() + " to " + maxLength + " chars");
			s = s.substring(0, maxLength);
		}
		return s;
	}

	private static String sanitizeNativeId(String nativeId) {
		if (nativeId.length() > 256) {
			String surrogateId = UUID.randomUUID().toString();
			logger.warning("native ID '" + nativeId + "' exceeds maximum length, substituting generated ID '" +
					surrogateId + "'");
			return surrogateId;
		}
		return nativeId;
	}

	private int saveUserProfile(Connection conn, SocialMediaUser user) throws SQLException {
		PreparedStatement insertUser = conn.prepareStatement("insert into " + tablePrefix +
				"_USER(`nativeId`, `name`, `isFullyCrawled`, `numRelationships`) values(?, ?, ?, ?)",
				Statement.RETURN_GENERATED_KEYS);
		try {
			insertUser.setString(1, sanitizeNativeId(user.getId()));
			insertUser.setString(2, truncateString(user.getName(), 256, "user.name"));
			insertUser.setBoolean(3, user.isFullyCrawled());
			insertUser.setInt(4, user.getNumFriends());
			insertUser.executeUpdate();
			ResultSet res = insertUser.getGeneratedKeys();
			try {
				res.next();
				return res.getInt(1);
			} finally {
				res.close();
			}
		} finally {
			insertUser.close();
		}
	}

	private void saveUserRelationships(Connection conn, SocialMediaUser user, Map<String, Integer> userIds)
			throws SQLException {
		int id = userIds.get(user.getId());
		Set<SocialMediaUser> targetUsers = user.getFriends();
		List<Integer> targetIds = new ArrayList<Integer>(targetUsers.size());
		for (SocialMediaUser targetUser : targetUsers)
			targetIds.add(userIds.get(targetUser.getId()));

		PreparedStatement insertRelationships = conn.prepareStatement("insert into " + tablePrefix +
				"_RELATIONSHIPS(`from`, `to`) values(?, ?)");
		try {
			insertRelationships.setInt(1, id);
			for (Integer targetId : targetIds) {
				insertRelationships.setInt(2, targetId);
				insertRelationships.addBatch();
			}
			insertRelationships.executeBatch();
		} finally {
			insertRelationships.close();
		}
	}

	private int saveMessageData(Connection conn, SocialMediaMessage message, Map<String, Integer> userIds)
			throws SQLException {
		int senderId = userIds.get(message.getSender().getId());

		if (message.getContent().length() > (64 * 1024))
			logger.warning("length of message '" + message.getId() + "' exceeds 64K: " + message.getContent().length());

		PreparedStatement insertMessage = conn.prepareStatement("insert into " + tablePrefix +
				"_MESSAGE(`nativeId`, `date`, `sender`, `isReply`, `isShared`, `content`) values(?, ?, ?, ?, ?, ?)",
				Statement.RETURN_GENERATED_KEYS);
		try {
			insertMessage.setString(1, sanitizeNativeId(message.getId()));
			insertMessage.setTimestamp(2, new Timestamp(message.getDate().getTime()));
			insertMessage.setInt(3, senderId);
			insertMessage.setBoolean(4, message.isReply());
			insertMessage.setBoolean(5, message.isShared());
			if (!message.getContent().isEmpty())
				insertMessage.setString(6, message.getContent());
			else
				insertMessage.setNull(6, java.sql.Types.VARCHAR);
			insertMessage.executeUpdate();
			ResultSet res = insertMessage.getGeneratedKeys();
			try {
				res.next();
				return res.getInt(1);
			} finally {
				res.close();
			}
		} finally {
			insertMessage.close();
		}
	}

	private void saveMessageParent(Connection conn, SocialMediaMessage message, Map<String, Integer> messageIds)
			throws SQLException {
		int id = messageIds.get(message.getId());
		int parentId = messageIds.get(message.getParent().getId());
		PreparedStatement updateMessage = conn.prepareStatement("update " + tablePrefix + "_MESSAGE " +
				"set `parent` = ? where `id` = ?");
		try {
			updateMessage.setInt(1, parentId);
			updateMessage.setInt(2, id);
			updateMessage.executeUpdate();
		} finally {
			updateMessage.close();
		}
	}

	private void saveMessageOrigin(Connection conn, SocialMediaMessage message, Map<String, Integer> messageIds)
			throws SQLException {
		int id = messageIds.get(message.getId());
		int originId = messageIds.get(message.getOrigin().getId());
		PreparedStatement updateMessage = conn.prepareStatement("update " + tablePrefix + "_MESSAGE " +
				"set `origin` = ? where `id` = ?");
		try {
			updateMessage.setInt(1, originId);
			updateMessage.setInt(2, id);
			updateMessage.executeUpdate();
		} finally {
			updateMessage.close();
		}
	}

	private void saveMessageRecipients(Connection conn, SocialMediaMessage message, Map<String, Integer> userIds,
			Map<String, Integer> messageIds) throws SQLException {
		int id = messageIds.get(message.getId());
		Set<SocialMediaUser> targetUsers = message.getRecipients();
		List<Integer> targetIds = new ArrayList<Integer>(targetUsers.size());
		for (SocialMediaUser targetUser : targetUsers)
			targetIds.add(userIds.get(targetUser.getId()));

		PreparedStatement insertRelationships = conn.prepareStatement("insert into " + tablePrefix +
				"_RECIPIENTS(`message`, `recipient`) values(?, ?)");
		try {
			insertRelationships.setInt(1, id);
			for (Integer targetId : targetIds) {
				insertRelationships.setInt(2, targetId);
				insertRelationships.addBatch();
			}
			insertRelationships.executeBatch();
		} finally {
			insertRelationships.close();
		}
	}

	public void save(Collection<SocialMediaUser> users, Collection<SocialMediaMessage> messages) throws SQLException {
		Connection conn = getConnection();
		try {
			conn.setAutoCommit(false);
			Savepoint beforeSaving = conn.setSavepoint();
			try {
				Map<String, Integer> userIds = new HashMap<String, Integer>();
				for (SocialMediaUser user : users)
					userIds.put(user.getId(), saveUserProfile(conn, user));
				for (SocialMediaUser user : users)
					saveUserRelationships(conn, user, userIds);

				Map<String, Integer> messageIds = new HashMap<String, Integer>();
				for (SocialMediaMessage message : messages)
					messageIds.put(message.getId(), saveMessageData(conn, message, userIds));
				for (SocialMediaMessage message : messages) {
					if (message.getParent() != null)
						saveMessageParent(conn, message, messageIds);
					if (message.getOrigin() != null)
						saveMessageOrigin(conn, message, messageIds);
				}
				for (SocialMediaMessage message : messages)
					saveMessageRecipients(conn, message, userIds, messageIds);

				conn.commit();
			} catch (Exception ex) {
				conn.rollback(beforeSaving);
				throw new SQLException("error while saving social media data, rolled back to previous state", ex);
			}
		} finally {
			conn.close();
		}
	}

}
