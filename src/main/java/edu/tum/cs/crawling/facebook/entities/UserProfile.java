package edu.tum.cs.crawling.facebook.entities;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

public class UserProfile implements Serializable {

	private static final long serialVersionUID = 5220503759095434683L;

	private final Person user;
	private final PersonDetails details;
	private final List<Post> posts;
	private final int numFriends;
	private final Set<Person> friends;
	private final Set<Site> likedsites;
	private final String language;

	public UserProfile(Person user, PersonDetails details, List<Post> posts, int numFriends, Set<Person> friends,
			Set<Site> likedsites, String language) {
		this.user = user;
		this.details = details;
		this.posts = posts;
		this.numFriends = numFriends;
		this.friends = friends;
		this.likedsites = likedsites;
		this.language = language;
	}

	public Person getUser() {
		return user;
	}

	public PersonDetails getDetails() {
		return details;
	}

	public List<Post> getPosts() {
		return posts;
	}

	public int getNumFriends() {
		return numFriends;
	}

	public Set<Person> getFriends() {
		return friends;
	}

	public Set<Site> getLikedsites() {
		return likedsites;
	}

	public String getLanguage() {
		return language;
	}

}
