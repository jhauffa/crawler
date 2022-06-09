package edu.tum.cs.postprocessing;

import java.util.HashSet;
import java.util.Set;

public class SocialMediaUser {

	private final String id;
	private final String name;
	private final boolean fullyCrawled;
	private final int numFriends;
	private final Set<SocialMediaUser> friends;

	public SocialMediaUser(String id, String name, boolean fullyCrawled, int numFriends,
			Set<SocialMediaUser> friends) {
		this.id = id;
		this.name = name;
		this.fullyCrawled = fullyCrawled;
		this.numFriends = numFriends;
		this.friends = friends;
	}

	public SocialMediaUser(String id, String name, boolean fullyCrawled, int numFriends) {
		this(id, name, fullyCrawled, numFriends, new HashSet<SocialMediaUser>());
	}

	public String getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public boolean isFullyCrawled() {
		return fullyCrawled;
	}

	/** @return overall number of friends at time of crawling, including those not returned by {@link #getFriends()} */
	public int getNumFriends() {
		return numFriends;
	}

	public Set<SocialMediaUser> getFriends() {
		return friends;
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof SocialMediaUser))
			return false;
		return id.equals(((SocialMediaUser) other).id);
	}

}
