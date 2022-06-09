package edu.tum.cs.crawling.twitter.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.tum.cs.crawling.twitter.entities.Tweet;
import edu.tum.cs.crawling.twitter.entities.TwitterUser;
import edu.tum.cs.crawling.twitter.protocol.ClientRequestObject;
import edu.tum.cs.crawling.twitter.protocol.ServerResponseObject;
import edu.tum.cs.crawling.twitter.protocol.ClientRequestObject.ClientRequestType;
import edu.tum.cs.crawling.twitter.protocol.ServerResponseObject.ServerStatus;

public class TestTwitterDumperServer {

	private static final String testDbUrl =
			"jdbc:mysql://localhost/crawler_unit_test?rewriteBatchedStatements=true&user=unittest&password=test";

	private static boolean isDbAvailable() {
		try {
			Connection conn = DriverManager.getConnection(testDbUrl);
			conn.close();
			return true;
		} catch (SQLException ex) {
			System.err.println("error connecting to test database");
			ex.printStackTrace();
			return false;
		}
	}

	private static boolean skipTest = false;

	@BeforeClass
	public static void startServer() throws IOException {
		if (isDbAvailable()) {
			System.setProperty("edu.tum.cs.crawling.twitter.dburl", testDbUrl);
			System.setProperty("hibernate.hbm2ddl.auto", "update");
			TwitterDumperServer.main(new String[0]);
		} else
			skipTest = true;
	}

	private static TwitterUser generateUser(String name, long id) {
		TwitterUser user = new TwitterUser();
		user.setId(id);
		user.setName(name);
		user.setScreenName(name.replace(' ', '_'));
		return user;
	}

	private static long baseUserId = 1337L;
	private static int maxUsers = 10;

	private static Collection<TwitterUser> generateUsers() {
		// create two users that follow each other
		long id = baseUserId;
		TwitterUser u1 = generateUser("John Smoth", id++);
		TwitterUser u2 = generateUser("Woll Smoth", id);
		u1.getFollowersFriends().addFollowers(new long[] { u2.getId() });
		u1.getFollowersFriends().addFriends(new long[] { u2.getId() });
		u2.getFollowersFriends().addFollowers(new long[] { u1.getId() });
		u2.getFollowersFriends().addFriends(new long[] { u1.getId() });

		Collection<TwitterUser> users = new ArrayList<TwitterUser>(2);
		users.add(u1);
		users.add(u2);
		return users;
	}

	private static long baseTweetId = 2000L;
	private static int maxTweets = 10;

	private static Collection<Tweet> generateTweets(Collection<TwitterUser> users) {
		// create a tweet posted by the first user
		long userId = users.iterator().next().getId();
		Tweet tweet = new Tweet();
		tweet.setId(baseTweetId);
		tweet.setUserId(userId);
		tweet.setStatusText("hello world");

		Collection<Tweet> tweets = new ArrayList<Tweet>(1);
		tweets.add(tweet);
		return tweets;
	}

	private static void sendRequest(ClientRequestObject cro) throws Exception {
		Socket socket = new Socket("localhost", TwitterDumperServer.port);
		try {
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
			oos.writeObject(cro);
			oos.flush();
			ServerResponseObject sro = (ServerResponseObject) ois.readObject();
			assertEquals(ServerStatus.OK, sro.getStatus());
		} finally {
			socket.close();
		}
	}

	@Test
	public void testServer() throws Exception {
		if (skipTest) {
			System.err.println("database not available, skipping test");
			return;
		}

		Collection<TwitterUser> users = generateUsers();
		Collection<Tweet> tweets = generateTweets(users);

		// send users and tweets
		ClientRequestObject cro = new ClientRequestObject();
		cro.setRequestType(ClientRequestType.DELIVER_TWEETS_AND_USERS);
		cro.getUsers().addAll(users);
		cro.getTweets().addAll(tweets);
		sendRequest(cro);

		// send followers and friends
		cro = new ClientRequestObject();
		cro.setRequestType(ClientRequestType.DELIVER_FOLLOWERS_AND_FRIENDS);
		cro.getUsers().addAll(users);
		sendRequest(cro);

		// check contents of database
		Connection conn = DriverManager.getConnection(testDbUrl);
		try {
			Statement st = conn.createStatement();

			ResultSet rs = st.executeQuery("select ID,FOLLOWERS_SER,FRIENDS_SER from `user` limit " + maxUsers);
			try {
				int numUsers = 0;
				while (rs.next()) {
					assertTrue(rs.getLong(1) >= baseUserId);
					assertNotNull(rs.getBytes(2));
					assertNotNull(rs.getBytes(3));
					numUsers++;
				}
				assertEquals(2, numUsers);
			} finally {
				rs.close();
			}

			rs = st.executeQuery("select ID,USER_ID from `tweet` limit " + maxTweets);
			try {
				int numTweets = 0;
				while (rs.next()) {
					assertTrue(rs.getLong(1) >= baseTweetId);
					assertEquals(baseUserId, rs.getLong(2));
					numTweets++;
				}
				assertEquals(1, numTweets);
			} finally {
				rs.close();
			}
		} finally {
			conn.close();
		}
	}

	private static final String[] tables = {
		"tweet", "user", "website", "waiting_user", "waiting_user_for_further_tweets"
	};

	@AfterClass
	public static void stopServer() throws SQLException {
		if (skipTest)
			return;

		TwitterDumperServer.terminate();

		// drop all generated tables
		Connection conn = DriverManager.getConnection(testDbUrl);
		try {
			Statement st = conn.createStatement();
			for (String table : tables) {
				try {
					st.executeUpdate("drop table " + table);
				} catch (SQLException ex) {
					System.err.println("error deleting table '" + table + "' from test database: " + ex.getMessage());
				}
			}
		} finally {
			conn.close();
		}
	}

}
