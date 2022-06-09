package edu.tum.cs.crawling.twitter.protocol;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class ServerResponseObject implements Serializable {

	public static class FollowFriendUser implements Serializable {
		private static final long serialVersionUID = 5720366811324818865L;

		public long id;
		public int totalFollowerCount;
		public int totalFriendCount;
	}

	public static class FurtherCrawlingUserData implements Serializable {
		private static final long serialVersionUID = 7490380928135093467L;

		public long id;
		public long firstTweetId;
	}

	private static final long serialVersionUID = 64615300572956143L;

	public enum ServerStatus {
		UNKOWN, OK, DELIVER_IDS, STATUS_ERROR
	}

	private Set<Long> ids = new HashSet<Long>();
	private Set<String> screenNames = new HashSet<String>();
	private ServerStatus status = ServerStatus.UNKOWN;
	private Set<FurtherCrawlingUserData> furtherCrawlingUsers = new HashSet<FurtherCrawlingUserData>();
	private Set<FollowFriendUser> followFriendUsers = new HashSet<FollowFriendUser>();

	public ServerStatus getStatus() {
		return status;
	}

	public void setStatus(ServerStatus status) {
		this.status = status;
	}

	public Set<Long> getIds() {
		return ids;
	}

	public Set<String> getScreenNames() {
		return screenNames;
	}

	public Set<FurtherCrawlingUserData> getFurtherCrawlingUsers() {
		return furtherCrawlingUsers;
	}

	public Set<FollowFriendUser> getFollowFriendsUsers() {
		return followFriendUsers;
	}

}
