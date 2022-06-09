package edu.tum.cs.crawling.twitter.protocol;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import edu.tum.cs.crawling.twitter.entities.Tweet;
import edu.tum.cs.crawling.twitter.entities.TwitterUser;

public class ClientRequestObject implements Serializable {

	private static final long serialVersionUID = -8750082178862610072L;

	public enum ClientRequestType {
		UNKNOWN, REQUEST_IDS, DELIVER_TWEETS_AND_USERS, DELIVER_FOLLOWERS_AND_FRIENDS, REQUEST_IDS_FOR_FURTHER_TWEETS,
		REQUEST_IDS_FOR_FOLLOWER_FRIENDS
	}

	private ClientRequestType requestType = ClientRequestType.UNKNOWN;
	private int numberOfIds;
	private Set<TwitterUser> users = new HashSet<TwitterUser>();
	private Set<Tweet> tweets = new HashSet<Tweet>();
	private Set<String> failedUserScreenNames = new HashSet<String>();

	public int getNumberOfIds() {
		return numberOfIds;
	}

	public void setNumberOfIds(int numberOfIds) {
		this.numberOfIds = numberOfIds;
	}

	public Set<TwitterUser> getUsers() {
		return users;
	}

	public Set<Tweet> getTweets() {
		return tweets;
	}

	public ClientRequestType getRequestType() {
		return requestType;
	}

	public void setRequestType(ClientRequestType requestType) {
		this.requestType = requestType;
	}

	public Set<String> getFailedUserScreenNames() {
		return failedUserScreenNames;
	}

}
