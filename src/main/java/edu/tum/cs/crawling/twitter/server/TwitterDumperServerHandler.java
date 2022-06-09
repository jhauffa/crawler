package edu.tum.cs.crawling.twitter.server;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.tum.cs.crawling.twitter.protocol.ClientRequestObject;
import edu.tum.cs.crawling.twitter.protocol.ServerResponseObject;
import edu.tum.cs.crawling.twitter.protocol.ServerResponseObject.ServerStatus;

class TwitterDumperServerHandler implements Runnable {

	private static final Logger logger = Logger.getLogger(TwitterDumperServerHandler.class.getName());

	private static final IdBroker idBroker = IdBroker.getInstance();

	private final Socket client;
	private final boolean addIdsToWaitingList;

	public TwitterDumperServerHandler(Socket client, boolean addIdsToWaitingList) {
		this.client = client;
		this.addIdsToWaitingList = addIdsToWaitingList;
	}

	@Override
	public void run() {
		try {
			ObjectOutputStream oos = new ObjectOutputStream(client.getOutputStream());
			ObjectInputStream ois = new ObjectInputStream(client.getInputStream());

			ClientRequestObject cro = (ClientRequestObject) ois.readObject();
			ServerResponseObject sro = new ServerResponseObject();

			switch (cro.getRequestType()) {
			case REQUEST_IDS:
				try {
					sro = idBroker.getIds(cro.getNumberOfIds());
					sro.setStatus(ServerStatus.DELIVER_IDS);
				} catch (Throwable e) {
					logger.log(Level.SEVERE, "Could not handle REQUEST_IDS request", e);
					sro.setStatus(ServerStatus.STATUS_ERROR);
				}
				break;

			case DELIVER_TWEETS_AND_USERS:
				try {
					logger.info("Got " + cro.getUsers().size() + " users and " + cro.getTweets().size() +
							" tweets: " + cro.hashCode());
					TwitterDao.saveUsers(cro.getUsers());
					TwitterDao.saveTweets(cro.getTweets());
					idBroker.extractIds(cro, addIdsToWaitingList);
					sro.setStatus(ServerStatus.OK);
					logger.info("Saved: " + cro.hashCode());
				} catch (Throwable e) {
					logger.log(Level.SEVERE, "Could not handle DELIVER_TWEETS_AND_USERS request", e);
					sro.setStatus(ServerStatus.STATUS_ERROR);
				}
				break;

			case DELIVER_FOLLOWERS_AND_FRIENDS:
				try {
					logger.info("Got " + cro.getUsers().size() + " users: " + cro.hashCode());
					TwitterDao.saveFollowersAndFriends(cro.getUsers());
					idBroker.extractIds(cro, addIdsToWaitingList);
					sro.setStatus(ServerStatus.OK);
					logger.info("Saved: " + cro.hashCode());
				} catch (Throwable e) {
					logger.log(Level.SEVERE, "Could not handle DELIVER_FOLLOWERS_AND_FRIENDS request", e);
					sro.setStatus(ServerStatus.STATUS_ERROR);
				}
				break;

			case REQUEST_IDS_FOR_FURTHER_TWEETS:
				try {
					sro = idBroker.getIdsForFurtherCrawling(cro.getNumberOfIds());
					sro.setStatus(ServerStatus.DELIVER_IDS);
				} catch (Throwable e) {
					logger.log(Level.SEVERE, "Could not handle REQUEST_IDS_FOR_FURTHER_TWEETS request", e);
					sro.setStatus(ServerStatus.STATUS_ERROR);
				}
				break;

			case REQUEST_IDS_FOR_FOLLOWER_FRIENDS:
				try {
					sro = idBroker.getIdsForFollowerFriends(cro.getNumberOfIds());
					sro.setStatus(ServerStatus.DELIVER_IDS);
				} catch (Throwable e) {
					logger.log(Level.SEVERE, "Could not handle REQUEST_IDS_FOR_FOLLOWER_FRIENDS request", e);
				}
				break;

			case UNKNOWN:
			default:
				logger.warning("Unknown cro.getRequestType(): " + cro.getRequestType());
				sro.setStatus(ServerStatus.STATUS_ERROR);
				break;
			}

			oos.writeObject(sro);
			oos.flush();
			oos.close();
			ois.close();
			client.close();
		} catch (IOException e) {
			logger.log(Level.SEVERE, "IOException:" + e.getMessage(), e);
		} catch (ClassNotFoundException e) {
			logger.log(Level.SEVERE, "ClassNotFoundException: " + e.getMessage(), e);
		} finally {
			if (!client.isClosed()) {
				try {
					client.close();
				} catch (IOException e) {
					// ignore
				}
			}
		}
	}

}
