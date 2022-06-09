package edu.tum.cs.crawling.facebook.entities;

import java.io.Serializable;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class Comment implements Serializable {

	private static final long serialVersionUID = -2072149212520567536L;

	private final Person sender;
	private final Date time;
	private final String text;
	private final Set<Person> likes = new HashSet<Person>();	// people who liked the comment

	public Comment(Person sender, Date time, String text) {
		this.sender = sender;
		this.time = time;
		this.text = text;
	}

	public Person getSender() {
		return sender;
	}

	public Date getTime() {
		return time;
	}

	public String getText() {
		return text;
	}

	public Set<Person> getLikes() {
		return likes;
	}

}
