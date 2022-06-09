package edu.tum.cs.crawling.twitter.server;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.exception.DataException;
import org.hibernate.service.ServiceRegistry;

import com.mchange.v2.c3p0.DataSources;

import edu.tum.cs.crawling.twitter.entities.*;

public class TwitterDao {

	private static final Logger logger;
	private static final SessionFactory sessionFactory;
	private static DataSource pooledDataSource;

	static {
		logger = Logger.getLogger(TwitterDao.class.getName());

		Configuration configuration = new Configuration();
		configuration.configure();	// load from resources
		String dbUrl = System.getProperty("edu.tum.cs.crawling.twitter.dburl");
		if (dbUrl != null)
			configuration.setProperty(AvailableSettings.URL, dbUrl);
		else
			dbUrl = configuration.getProperty(AvailableSettings.URL);
		String hbm2ddl = System.getProperty("hibernate.hbm2ddl.auto");
		if (hbm2ddl != null)
			configuration.setProperty(AvailableSettings.HBM2DDL_AUTO, hbm2ddl);

		ServiceRegistry serviceRegistry =
				new StandardServiceRegistryBuilder().applySettings(configuration.getProperties()).build();
		sessionFactory = configuration.buildSessionFactory(serviceRegistry);

		try {
			DataSource unpooledDataSource = DataSources.unpooledDataSource(dbUrl);
			pooledDataSource = DataSources.pooledDataSource(unpooledDataSource);
		} catch (SQLException e) {
			logger.log(Level.SEVERE, "initializing database", e);
		}
	}

	/**
	 * @return Connection from the pool, close the connection if you don't use it anymore!
	 */
	public static Connection getConnection() throws SQLException {
		return pooledDataSource.getConnection();
	}

	public static TwitterUser getUser(long id) {
		Session session = sessionFactory.openSession();
		try {
			return (TwitterUser) session.byId(TwitterUser.class).load(id);
		} finally {
			session.close();
		}
	}

	public static List<Tweet> getTweetsOfUser(long userId) {
		Session session = sessionFactory.openSession();
		try {
			Query query = session.createQuery("from Tweet where user_id = :user_id");
			query.setLong("user_id", userId);
			return query.list();
		} finally {
			session.close();
		}
	}

	public static Tweet getOriginalTweet(Tweet tweet) {
		Session session = sessionFactory.openSession();
		try {
			// Tweet.getRetweetOfStatusId always points directly to the original tweet
			Tweet originalTweet = (Tweet) session.byId(Tweet.class).load(tweet.getRetweetOfStatusId());
			if (originalTweet != null)
				return originalTweet;
			return tweet;
		} finally {
			session.close();
		}
	}

	public static void saveUsers(Collection<TwitterUser> users) throws SQLException, IOException {
		Session session = sessionFactory.openSession();
		try {
			Transaction transaction = session.beginTransaction();
			for (TwitterUser user : users) {
				TwitterUser oldUser = getUser(user.getId());
				if (oldUser != null) {
					if (oldUser.getFirstCrawledAt().before(user.getFirstCrawledAt()))
						user.setFirstCrawledAt(oldUser.getFirstCrawledAt());
					if (user.getDetectedLanguage() == null)
						user.setDetectedLanguage(oldUser.getDetectedLanguage());
				}
				session.saveOrUpdate(user);
			}
			transaction.commit();
		} finally {
			session.close();
		}
	}

	public static void saveTweets(Collection<Tweet> tweets) {
		Session session = sessionFactory.openSession();
		try {
			Transaction transaction = session.beginTransaction();
			for (Tweet tweet : tweets) {
				Tweet oldTweet = (Tweet) session.byId(Tweet.class).load(tweet.getId());
				if (oldTweet != null) {
					tweet.setFirstCrawledAt(oldTweet.getFirstCrawledAt());
					tweet.setRetweetCount(Math.max(tweet.getRetweetCount(), oldTweet.getRetweetCount()));
					/*
					if (tweet.getJSONSource() == null && oldTweet.getJSONSource() != null)
						tweet.setJSONSource(oldTweet.getJSONSource());
					 */
					session.update(tweet);
				} else {
					session.save(tweet);
				}
			}
			transaction.commit();
		} finally {
			session.close();
		}
	}

	/**
	 * Saves the users' Friends and Followers (and only this information) to the user table, the users must exist.
	 */
	public static void saveFollowersAndFriends(Collection<TwitterUser> users) {
		Session session = sessionFactory.openSession();
		try {
			Transaction transaction = session.beginTransaction();
			for (TwitterUser user : users) {
				FollowersFriends ff = user.getFollowersFriends();
				if (!ff.getFollowers().isEmpty() || !ff.getFriends().isEmpty())
					session.update(ff);
			}
			transaction.commit();
		} finally {
			session.close();
		}
	}

	public static int saveWebsites(List<Website> websites) {
		int numFailures = 0;

		Session session = sessionFactory.openSession();
		try {
			Transaction transaction = session.beginTransaction();
			for (Website website : websites) {
				try {
					if ((website.getContent() != null) && website.getContent().isEmpty())
						website.setContent(null);
					session.save(website);
				} catch (DataException ex) {
					logger.log(Level.SEVERE, "tweetID: " + website.getTweetId() +
							", statusCode: " + website.getStatusCode() + ", originalUrl: " + website.getOriginalUrl() +
							", resolvedUrl: " + website.getOriginalUrl(), ex);
					numFailures++;
				}
			}
			transaction.commit();
		} finally {
			session.close();
		}

		return numFailures;
	}

}
